""""
Module to take as input parsings of MCI IGM clinical reports 
And map them to genetic_analysis node props

"""

import os, sys, json
from datetime import datetime
from prefect import task, flow, get_run_logger


# take as input the parsed MCI IGM clinical reports path (long format), sample mapping path and manifest XLSX path

# need tumor_normal somatic reports and fusion reports

# 0. map props of node to IGM data elements



# 1. generate genetic_analysis node_id from participant, sample and order from 


# 2. 