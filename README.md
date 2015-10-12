# database-vertical-partitioning
Vertical partitioning algorithms used in physical design of databases.

Vertical partitioning is a crucial step in physical database design in row-oriented databases. A number of vertical partitioning algorithms have been proposed over the last three decades for a variety of niche scenarios. In principle, the underlying problem remains the same: decompose a table into one or more vertical partitions.

We implemented the following 6 + 1 algorithms in Java:
- AutoPart [S. Papadomanolakis and A. Ailamaki. AutoPart: Automating Schema Design for Large Scientific Databases Using Data Partitioning. In SSDBM, pages 383–392, 2004.]
- HillClimb [R. A. Hankins and J. M. Patel. Data Morphing: An Adaptive, Cache-Conscious Storage Technique. In VLDB, pages 417–428, 2003.]
- HYRISE [M. Grund, J. Krüger, H. Plattner, A. Zeier, P. Cudre-Mauroux, and S. Madden. HYRISE: A Main Memory Hybrid Storage Engine. PVLDB, 4(2):105–116, 2010.]
- Navathe [S.Navathe, S.Ceri, G.Wiederhold, and J.Dou. Vertical Partitioning Algorithms for Database Design. ACM TODS, 9(4):680–710, 1984.]
- O2P [A.Jindal and J.Dittrich. Relax and let the database do the partitioning online. In BIRTE, pages 65–80, 2011.]
Trojan [A.Jindal, J.-A. Quianeé-Ruiz, and J.Dittrich. Trojan Data Layouts: Right Shoes for a Running Elephant. In ACM - SOCC, pages 21:1–21:14, 2011.]
- Brute Force

Read about our findings for legacy row-store database systems in our VLDB'13 paper [Jindal, A., Palatinus, E., Pavlov, V., & Dittrich, J. (2013). A comparison of knives for bread slicing. Proceedings of the VLDB Endowment, 6(6), 361-372.]:
http://www.vldb.org/pvldb/vol6/p361-jindal.pdf
