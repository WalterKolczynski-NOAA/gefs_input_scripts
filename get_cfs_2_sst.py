#! /usr/bin/env python3

import sys, os, datetime, subprocess
import bisect
from subprocess import call
from datetime import timedelta
import dateutil.rrule
from dateutil.rrule import rrule, HOURLY

cfs_runhistory_filepattern = "/NCEPPROD/hpssprod/runhistory/cfs%Y/%Y%m/%Y%m%d/time/cfs.%Y%m%d%H.m01.time.tar"
destination_path = "/scratch4/NCEPDEV/ensemble/noscrub/Walter.Kolczynski/gefs_input/cfs_2_sst"
scheduler_command = "sbatch --job-name get_cfs_sst -p {partition} --qos={qos} -A {account} -n 1 -t {walltime} --mem={vmem} --array=0-{last_job_id}%{max_jobs} -o {outfile_name} --chdir={cwd} {script} {datearray_string}"
#scheduler_command = "bsub -J 'get_cfs_sst[0-{last_job_id}]%{max_jobs}' -P {account} -q {queue} -M {vmem} -R span[ptile=1] -R 'affinity[core(1)]' -n 1 -W {walltime} -o {outfile_name} -cwd {cwd} {script} {datearray_string}"

partition = "service"
qos = "batch"
account = "fv3-cpu"
vmem = "750M"
walltime = "1:30:00"
max_jobs = 2

def usage():
	print(
		"""
	Program to retrieve required files for GEFS initialization from HPSS

	Usage: getInit.py start_time [end_time interval]
	start_time:  First initialization time in YYYYMMDDHH format
	end_time:    Final initalization time in YYYYMMDDHH format (default: start_time)
	interval:    Frequency of initial conditions (default: 24)

		""")

if os.environ.get("SLURM_ARRAY_TASK_ID") != None:
	if( len(sys.argv) > 1 ):
		time_array = sys.argv[1].split(",")
	else:
		print("FATAL: No time array provided for array job!")
		usage()
		exit(-101)
	array_id = int(os.environ.get("SLURM_ARRAY_TASK_ID"))
	if(len(time_array) <= array_id):
		print("FATAL: time_array shorter than SLURM_ARRAY_TASK_ID")
		exit(-101)
	start_string = time_array[array_id]
	end_string = start_string
	interval = 24
else:
	if( len(sys.argv) > 1 ):
		start_string = sys.argv[1]
	else:
		print("FATAL: Must provide at least a start time!")
		usage()
		exit(-1)

	if( len(sys.argv) > 2 ):
		end_string = sys.argv[2]
	else:
		end_string = start_string

	if( len(sys.argv) > 3 ):
		interval = int(sys.argv[3])
	else:
		interval = 24

	if( len(start_string) == 8 ):
		start_string = start_string + "00"
	if( len(end_string) == 8 ):
		end_string = end_string + "00"

	if( len(start_string) != 10):
		"FATAL: Incorrect syntax for start_string!"
		usage()
		exit(-3)
	if( len(end_string) != 10):
		"FATAL: Incorrect syntax for end_string!"
		usage()
		exit(-3)

start_time = datetime.datetime.strptime(start_string, "%Y%m%d%H")
end_time = datetime.datetime.strptime(end_string, "%Y%m%d%H")

if(interval%6 != 0):
	print("FATAL: Invalid interval specified, must be divisible by 6!")
	usage()
	exit(-2)

if( start_time.hour % 6 != 0 ):
	print("FATAL: Invalid start time specified, only 00, 06 12, and 18 supported.")
	usage()
	exit(-2)

times = rrule(freq=HOURLY, interval=interval, dtstart=start_time, until=end_time )
n_times = times.count()

if n_times == 1:
	time = times[0]
	time_prevcyc = time + timedelta(hours=-6)
	time_prevday = time + timedelta(hours=-24)

	print("Current cycle: " + time.strftime("%Y %m %d %H"))
	print("Previous cycle: " + time_prevcyc.strftime("%Y %m %d %H"))

		# Get SST files from current SFS cycle
	destination = time_prevday.strftime(destination_path + "/cfs.%Y%m%d/%H/time_grib_01")
	os.makedirs(destination, exist_ok=True)
	os.chdir(destination)

	files = [
		time_prevday.strftime("tmpsfc.01.%Y%m%d%H.daily.grb2")
	]

	tarfile = time_prevday.strftime(cfs_runhistory_filepattern)

	print("Extracting " + tarfile)
	# print("htar -xf " + tarfile + " " + " ".join(files))
	call("htar -xf " + tarfile + " " + " ".join(files), shell=True)

else:
	cwd = os.path.dirname(os.path.abspath(__file__))
	datearray_string = ",".join([time.strftime("%Y%m%d%H") for time in times])
	last_job_id = n_times-1
	script = os.path.abspath(__file__)
	os.makedirs(cwd + "/logs", exist_ok=True)
	outfile_name = "{cwd}/logs/{script}_{start}_{end}.log".format(cwd=cwd, script=os.path.splitext(os.path.basename(script))[0], start=start_string, end=end_string)
	p = subprocess.Popen(scheduler_command.format(
		partition=partition,
		qos=qos,
		account=account, 
		walltime=walltime, 
		vmem=vmem, 
		last_job_id=last_job_id, 
		max_jobs=max_jobs, 
		outfile_name=outfile_name,
		cwd=cwd, 
		datearray_string=datearray_string, 
		script=script
		), shell=True)
	p.wait()
	print("{}".format(p.args))

exit(0)