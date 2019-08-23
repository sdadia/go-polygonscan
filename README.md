# TRIP-CALCULATION

Repository containing all resources required for Trip Calculation functionality for pvcam-cloud implementation given enriched, decompressed telematics data. 

Please reference 'Architecture/ServicesDiagram.png' for a architecture diagram of this service in its current state.

To Run Unit Tests:
python3.6 -m unittest discover Tests/UnitTests/

Any tests stored in the 'Tests/IntegrationTests' directory will be executed before deployment of this service as part of a full regression test before deployment into production environments. 