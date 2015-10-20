package core.costmodels;

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import db.schema.entity.Workload;


/**
 * Cost Model from the HYRISE cost model.
 * 
 *  Note:
 *  1. We consider only partial projectedColumns without any selections in the queries.
 *  2. Each query might access non-contiguous attributes within a partition.
 *  3. The first and last projection (within a partition) could also be merged.
 *  
 * @author alekh
 */
public class MMCostModel {

	public static final int l1CacheLineWidth = 64; //32 * 1024;
	public static final int l2CacheLineWidth = 64; //6 * 1024 * 1024;

    private Workload.SimplifiedWorkload w;
	
	public MMCostModel(Workload.SimplifiedWorkload w) {
		this.w = w;
	}

    /**
     * Compute the cost for accessing a given partition over a given query
     * 
     * @param partition
     * @param query
     * @return
     */
    public double getCostForPartition(int[] partition, int query){

        TIntArrayList partitionList = new TIntArrayList(partition);
        partitionList.sort();
        partition = partitionList.toArray();
    	
    	// attributes referenced by the given query
    	int[] referencedAttributes = w.usageMatrix[query];
    	
    	// mask to denote whether or not an attribute of a partition is referenced (by the given query)
    	int[] accessMask = new int[partition.length];

        // does the query reference any attributes of the given partition?
        boolean hasReferencedAttrs = false;
    	
    	// set the access mask
    	for(int i=0;i<partition.length;i++) {
    		if (referencedAttributes[partition[i]]==1) {
    			accessMask[i] = 1;
                hasReferencedAttrs = true;
            }
        }

        if (!hasReferencedAttrs) {
            return 0.0;
        }
    	
    	int partitionSize = 0;
    	int gapOffset = 0;
    	int gapWidth = 0;
    	boolean gap = false;
    	TIntList gapOffsets = new TIntArrayList();
    	TIntList gapWidths = new TIntArrayList();
    	
    	// compute gap offsets and widths
    	for(int i=0;i<accessMask.length;i++){
    		if(accessMask[i]==0){	// non-referenced attribute
    			if(!gap){
    				// start of gap
    				gapWidth = 0;
    				gap = true;
    			}
    			gapWidth += w.attributeSizes[partition[i]];
    		}
    		else{		// referenced attribute
    			if(gap){
    				// end of gap
    				gapOffsets.add(gapOffset);
    				gapWidths.add(gapWidth);
    				gapOffset += gapWidth;
    				gap = false;
    			}
    			gapOffset += w.attributeSizes[partition[i]];
    		}
    		partitionSize += w.attributeSizes[partition[i]];
    	}
    	if(gap){
			// end of the last gap
			gapOffsets.add(gapOffset);
			gapWidths.add(gapWidth);
    	}

        // Make sure there is a dummy zero-width gap at the begining/end, if the first/last attribute is projected.
        if (gapOffsets.size() == 0 || gapOffsets.get(0) > 0) {
            TIntArrayList newGapOffsets = new TIntArrayList();
            newGapOffsets.add(0);
            newGapOffsets.add(gapOffsets.toArray());
            gapOffsets = newGapOffsets;

            TIntArrayList newGapWidths = new TIntArrayList();
            newGapWidths.add(0);
            newGapWidths.add(gapWidths.toArray());
            gapWidths = newGapWidths;
        }
        if (gapOffset == partitionSize) {
            gapOffsets.add(partitionSize);
            gapWidths.add(0);
        }

        // TODO Add the costs of reading data into the L1 cache as well.
        // TODO Multiple this with the cost of a cache miss.

    	return getCacheMisses(gapOffsets.toArray(), gapWidths.toArray(), partitionSize, w.numRows, 0, l2CacheLineWidth);
    }
    
    
    /**
     * Get the number of cache misses, assuming all projected attributes are not 
     * stored contiguously.
     *
     * @param gapOffsets - offsets for all gaps, including the ones in the beginning and at the end
     * @param gapWidths - widths for all gaps, including the ones in the beginning and at the end
     * @param containerWidth - width of the container (vertical partition)
     * @param containerRows - number of rows in the container (vertical partition)
     * @param containerOffset - mis-alignment of the container from the cache lines
     * @param cacheLineWidth - width of the cache lines under consideration
     * @return
     */
    private long getCacheMisses(int[] gapOffsets,
    							int[] gapWidths,
    							int containerWidth,
    							long containerRows, 
    							int containerOffset, 
    							int cacheLineWidth) {

        long misses = 0;
    	
    	// point to the first partial projection offset (should be 0,
    	//  if there exists a placeholder first gap even if it is zero in width)
    	int partialProjectionOffset = gapOffsets[0];
    	
    	// flag to indicate whether it is the first (gap) skip or not
    	boolean firstSkip = true;
    	
    	// flag to indicate whether or not we can merge the first and the last partial projectedColumns
    	boolean mergeFirstLast = false;
    	if ((gapWidths[0] + gapWidths[gapWidths.length-1]) < cacheLineWidth)
    		mergeFirstLast = true;	// first and last partial projectedColumns can be merged, and the gap between them cannot be skipped
    	else {
    		partialProjectionOffset += gapWidths[0];
    		firstSkip = false; /* since the first and last partial projectedColumns cannot be merged,
    		                     we skip the leftmost gap, so the next one won't be the first (gap) skip */
    	}

        // if there is only one series of contiguous attributes projected we skip the rest of this method
        if (gapOffsets.length == 2) {
            int partialProjectionWidth = gapOffsets[1] - gapWidths[0];

            return getCacheMisses(partialProjectionWidth, gapWidths[0], containerWidth,
                    containerRows, containerOffset, cacheLineWidth);
        }
    		
    	// width of first-last merged partial projection
    	int firstLastWidth = 0;

        // some gaps between the first and last one were skipped
        boolean skippedInBetween = false;
    	
    	for (int i=1;i<gapOffsets.length-1;i++) {
    		if(gapWidths[i] >= cacheLineWidth) { // can skip this gap

                skippedInBetween = true;
    			
    			int partialProjectionWidth = gapOffsets[i] - partialProjectionOffset;

    			if (mergeFirstLast && firstSkip)
    				firstLastWidth += partialProjectionWidth; 
    			else
    				misses += getCacheMisses(partialProjectionWidth, partialProjectionOffset, containerWidth, containerRows, containerOffset, cacheLineWidth);
    			
    			partialProjectionOffset = gapOffsets[i] + gapWidths[i];
    			firstSkip = false;
    		}
    	}
    	
    	if (mergeFirstLast){
    		firstLastWidth += (containerWidth - partialProjectionOffset);
    		misses += getCacheMisses(firstLastWidth, partialProjectionOffset, containerWidth, containerRows, containerOffset, cacheLineWidth);
    	} else if (!skippedInBetween) {
            misses += getCacheMisses(gapOffsets[gapOffsets.length - 1] - gapWidths[0], gapWidths[0], containerWidth,
                    containerRows, containerOffset, cacheLineWidth);
        }
    	
    	return misses;
    }
    

    /**
     * Get the number of cache misses, assuming that
     * a series of contiguous attributes are projected 
     * in a container.
     *
     * @param projectionWidth - width of the projected attributes
     * @param containerWidth - total width of the vertical partition
     * @param containerRows - number of rows in the vertical partition
     * @param containerOffset - mis-alignment of the container from the cache lines
     * @param cacheLineWidth - width of the cache lines under consideration
     * @return
     */
    private long getCacheMisses(int projectionWidth,
    							int projectionOffset,
    							int containerWidth, 
    							long containerRows, 
    							int containerOffset, 
    							int cacheLineWidth) {
    	
    	long misses = 0;
    	
    	if((containerWidth-projectionWidth) < cacheLineWidth){
    		// non-projected segments of the container cannot be skipped
    		misses = (long)Math.ceil( (double)(containerWidth*containerRows + containerOffset)/cacheLineWidth);
    	}
    	else{
    		// parts of the container can be skipped
    		long v = cacheLineWidth / GCD(containerWidth, cacheLineWidth);
    		for(long r=0;r<v;r++){
    			long rowOffset = containerWidth * r;
    			long lineOffset = (containerOffset + rowOffset + projectionOffset) % cacheLineWidth;
    			misses += (long)Math.ceil((double)(lineOffset + projectionWidth)/cacheLineWidth);
    		}
    		misses = (long)(misses * (double)containerRows / v);
    	}
    	
    	return misses;
    }
    
    private int GCD(int a, int b){
    	if (b==0) 
    		return a;
    	else
    		return GCD(b,a%b);
    }

    @Override
    public MMCostModel clone() {
        return new MMCostModel(this.w);
    }
}
