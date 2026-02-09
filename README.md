
# automation of deep springs metering information
February 2026
Lucinda DS'25

## phase 1: network drive to local storage and processing

# copy_acquisuite_logs.ps1 checks the campus file server for new log files and then copies them to the computer

# load_acquisuite_any_csv.py then goes through that local folder and parses through the new files and inserts them into a table called "measurements" in postgresSQL

#  run_pipeline.ps1 combines the two files into one script and creates a log to allow for both to be deployed at the same time and in a coordinated manner. 

For now, I have run_pipeline.ps1 set up on a PC to schedule deploy twice a day when the new log files are uploaded to the file server. 


