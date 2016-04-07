from __future__ import print_function
from pdb import set_trace as stop
import os
import argparse
#import logging
from collections import defaultdict
#logging.basicConfig()

#BaseSpace API imports
from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI
from BaseSpacePy.api.BaseSpaceAPI import QueryParameters

#Aggregate of local utility functions
from Util import *

#Individual download functions 
from run_folders import downloadRun
from project_folders import downloadProjectBam, downloadProjectFastq

def CLI(myAPI, inProjects, inRuns, dryRun, force):
    '''
    command line interface with the program
    allow user to select runs, projects, and/or fastq/bam files
    '''
    
    TotalSize = 0
    qp = QueryParameters.QueryParameters({'Limit':1024})
    scope = raw_input("select projects [p], samples [s], or runs [r]: " )
    while scope not in ['p', 'r', 's']:
        print("invalid selection")
        scope = raw_input("select projects [p], samples [s], or runs [r]: ")
    if scope == 'p' or scope == 's':
        projects = pickSomething("project(s)", inProjects)
        filetype = raw_input("bam [b] or fastq [f]: ")
        while filetype not in ['b', 'f']:
            print("invalid selection")
            filetype = raw_input("bam [b] or fastq [f]: ")
       
        if filetype == "b":
            if scope == 'p':
                for project in projects:
                    TotalSize += downloadProjectBam(project , myAPI, dryRun, force=force)
            elif scope == 's':
                # select sample(s)
                for project in projects:
                    samples = pickSomething("sample(s)", project.getSamples(myAPI, qp))
                    TotalSize += downloadProjectBam(project, myAPI, dryRun, force=force, samples=samples)
        elif filetype == "f":
            if scope == 'p':
                for project in projects:
                    TotalSize += downloadProjectFastq(project , myAPI, dryRun, force=force)
            elif scope == 's':
                # select sample(s)
                for project in projects:
                    samples = pickSomething("sample(s)", project.getSamples(myAPI, qp))
                    TotalSize += downloadProjectFastq(project, myAPI, dryRun, force=force, samples=samples)
        if len(projects) > 1:
            print(humanFormat(TotalSize) + "\tTotal")

    if scope == 'r':
        runs = pickSomething("run(s)", inRuns)
        for run in runs:
            TotalSize += downloadRun(run, myAPI, dryRun, force=force)
        if len(runs) > 1:
            print(humanFormat(totalSize) + "\tTotal")
    finished = raw_input("finished? (y/n): ")
    while finished not in ['y', 'n']:
            print("invalid selection")
            finished = raw_input("finished? (y/n): ")
    if finished == "n":
        CLI(myAPI, inProjects, inRuns, dryRun, force)
    
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--profile', default="DEFAULT", help="the .basespacepy.cfg profile to load")
    parser.add_argument('-d', '--dry', action='store_true', default=False, help="dry run; return size of selected items")
    parser.add_argument('-f', '--force', action='store_true', default=False, help="force overwrite; otherwise cat counters on new filenames")
    parser.add_argument('-i', '--input', required=False, help="WIP: tab delimited file of project...sample to download ")

    args = parser.parse_args()
    myAPI = BaseSpaceAPI(profile=args.profile, timeout=500)
    user = myAPI.getUserById('current')
    qp = QueryParameters.QueryParameters({'Limit':1024})

    projects = user.getProjects(myAPI, qp)
    runs = user.getRuns(myAPI, qp)
    if not args.input:
        CLI(myAPI, projects, runs, args.dry, args.force )
    else:
        userInputs = defaultdict(list)
        for line in open(args.input, 'r'):
            things = line.split()
            userInputs[things[0].strip()].append(things[1].strip())
        userProjNames = userInputs.keys()
        userProjs = [proj for proj in projects if str(proj) in userProjNames]
        for userProj in userProjs:
            samples = userInputs[userProj]
            downloadProjectFastq(userProj, myAPI, args.dry, force=args.force, samples=samples)

if __name__ == "__main__":
    main()
