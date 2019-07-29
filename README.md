# spark-coding-ground
This Coding project reads log data from FTP URL and process in Spark framework to derive metrics. Data Wrangling clean up and validations are handled by internal logic

# Results

These are captured from the spark run locally. DisplaySize and top records size to display can be configured to runtime arguments.

1. Daily Top visits by url


 
+-----------+----------------------------+----------+
|requestDate|endPoint                    |freq_count|
+-----------+----------------------------+----------+
|1995-07-06 |/images/NASA-logosmall.gif  |6227      |
|1995-07-06 |/images/KSC-logosmall.gif   |5204      |
|1995-07-06 |/shuttle/countdown/count.gif|3495      |
|1995-07-06 |/shuttle/countdown/         |3469      |
|1995-07-06 |/images/MOSAIC-logosmall.gif|2974      |
|1995-07-05 |/images/NASA-logosmall.gif  |6175      |
|1995-07-05 |/images/KSC-logosmall.gif   |5154      |
|1995-07-05 |/shuttle/countdown/count.gif|3637      |
|1995-07-05 |/shuttle/countdown/         |3587      |
|1995-07-05 |/images/MOSAIC-logosmall.gif|2795      |
|1995-07-07 |/images/NASA-logosmall.gif  |5055      |
|1995-07-07 |/images/KSC-logosmall.gif   |4136      |
|1995-07-07 |/images/MOSAIC-logosmall.gif|2823      |
|1995-07-07 |/images/USA-logosmall.gif   |2781      |
|1995-07-07 |/images/WORLD-logosmall.gif |2770      |
|1995-07-27 |/images/NASA-logosmall.gif  |3270      |
|1995-07-27 |/images/KSC-logosmall.gif   |2793      |
|1995-07-27 |/images/USA-logosmall.gif   |2520      |
|1995-07-27 |/images/MOSAIC-logosmall.gif|2512      |
|1995-07-27 |/images/WORLD-logosmall.gif |2494      |
+-----------+----------------------------+----------+

2. Showing Top Visits to server by visitor(Host address)

+-----------+------------------------+----------+
|requestDate|visitor                 |freq_count|
+-----------+------------------------+----------+
|1995-07-06 |piweba3y.prodigy.com    |732       |
|1995-07-06 |alyssa.prodigy.com      |682       |
|1995-07-06 |piweba1y.prodigy.com    |433       |
|1995-07-06 |spidey.cor.epa.gov      |401       |
|1995-07-06 |webgate1.mot.com        |379       |
|1995-07-05 |news.ti.com             |826       |
|1995-07-05 |piweba3y.prodigy.com    |664       |
|1995-07-05 |alyssa.prodigy.com      |473       |
|1995-07-05 |advantis.vnet.ibm.com   |440       |
|1995-07-05 |gateway.cary.ibm.com    |349       |
|1995-07-07 |piweba3y.prodigy.com    |879       |
|1995-07-07 |alyssa.prodigy.com      |767       |
|1995-07-07 |piweba1y.prodigy.com    |546       |
|1995-07-07 |163.206.89.4            |531       |
|1995-07-07 |webgate1.mot.com        |405       |
|1995-07-27 |edams.ksc.nasa.gov      |283       |
|1995-07-27 |192.223.3.88            |252       |
|1995-07-27 |jalisco.engr.ucdavis.edu|248       |
|1995-07-27 |piweba4y.prodigy.com    |241       |
|1995-07-27 |mega218.megamed.com     |239       |
+-----------+------------------------+----------+
