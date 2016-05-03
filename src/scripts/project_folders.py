from __future__ import print_function
from pdb import set_trace as stop
import os
import argparse
import shutil 
#BaseSpace API imports
from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI
from BaseSpacePy.api.BaseSpaceAPI import QueryParameters
#Aggregate of local utility functions
from Util import *

def downloadProjectFastq(project, myAPI, dryRun, samples=[], force=False, qp=QueryParameters.QueryParameters({'Limit':1024})):
    totalSize = 0
    if not samples:
        samples = project.getSamples(myAPI, qp)
    elif samples and type(samples[0]) == str: 
        #convert samples strings to sample objects
        samples = stringsToBSObj(project.getSamples(myAPI, qp), samples)
    for sample in samples:
        fns = sample.getFiles(myAPI, qp)
        for fn in fns:
            thisSize = fn.__dict__['Size']
            # skip addition until we know this will be downloaded
            #totalSize += thisSize                       
            if dryRun:
                totalSize += thisSize
                print(humanFormat(thisSize) + '\t' + fn.Name)
                continue
            savePath = str(project).replace(" ","_") + "/" + pathFromFile(fn, myAPI)
            tmpPath = str(project).replace(" ","_") + "/" + pathFromFile(fn, myAPI) + "partial/"            
            if not os.path.exists(savePath):
                os.makedirs(savePath)
            if not os.path.exists(tmpPath):
                os.makedirs(tmpPath)
            pathToFn = os.path.join(savePath, fn.Name)
            if not force and fileExists(pathToFn, fn):
                print("already have " + savePath + fn.Name + ". Skipping...")
                continue
            else:
                while os.path.exists(os.path.join(savePath, fn.Name)):
                    # if the path exists, append this string to the end to avoid overwriting
                    counter = 1
                    fn.Name = os.path.basename(fn.Path) + "." + str(counter)
                    counter += 1 
                totalSize += thisSize
                print(os.path.join(savePath, fn.Name))
                fn.downloadFile(myAPI, tmpPath)
                shutil.move(os.path.join(tmpPath,fn.Path) , os.path.join(savePath,fn.Name) )
    if os.path.exists(tmpPath) and not os.listdir(tmpPath):
        os.rmdir(tmpPath)                            
    print( humanFormat(totalSize) + '\t' + str(project) )
    return totalSize

def downloadProjectBam(project, myAPI, dryRun, samples=[], force=False, qp=QueryParameters.QueryParameters({'Limit':1024})):
    totalSize = 0
    results = project.getAppResults(myAPI, qp)
    for result in results:
        bams = [ x for x in result.getFiles(myAPI, qp) if "bam" in str(x) ]
        if samples:
            if type(samples[0]) == str:
                samples = stringsToBSObj(project.getSamples(myAPI, qp), samples)
            # user picked particular samples
            # subset the list of bams accordingly
            #bams = [x for x in bams if ]
            #WIP
            print("\n\nuser picked particular samples, but this isn't coded in yet\n")
            stop()
        for fn in bams:
            thisSize = fn.__dict__['Size']
            # totalSize += thisSize
            if dryRun:
                totalSize += thisSize
                print(humanFormat(thisSize) + '\t' + fn.Name)
                continue
            savePath = str(project).replace(" ","_") + "/" + pathFromFile(fn, myAPI)
            tmpPath = str(project).replace(" ","_") + "/" + pathFromFile(fn, myAPI) + "partial/"
            if not os.path.exists(savePath):
                os.makedirs(savePath)
            if not os.path.exists(tmpPath):
                os.makedirs(tmpPath)
            pathToFn = os.path.join(savePath, fn.Name)
            if not force and fileExists(pathToFn, fn):
                print("already have " + savePath + "/" + fn.Name + ". Skipping...")
                continue
            else:
                while os.path.exists(os.path.join(savePath, fn.Name)):
                    # if the path exists, append this string to the end to avoid overwriting
                    counter = 1
                    fn.Name = os.path.basename(fn.Path) + "." + str(counter)
                    counter += 1 
                print(os.path.join(savePath, fn.Name))
                totalSize += thisSize
                fn.downloadFile(myAPI, savePath)
                shutil.move(os.path.join(tmpPath,fn.Path) , os.path.join(savePath,fn.Name) )
    if os.path.exists(tmpPath) and not os.listdir(tmpPath):
        os.rmdir(tmpPath)        
    print( humanFormat(totalSize) + '\t' + str(project) )
    return totalSize
    
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--profile', default="DEFAULT", help="the .basespacepy.cfg profile to load")
    parser.add_argument('-d', '--dry', action='store_true', default=False, help="dry run; return size of selected items")
    parser.add_argument('-f', '--force', action='store_true', default=False, help="force overwrite; otherwise cat counters on new filenames")
    parser.add_argument('-j', '--project', required=True, nargs="+", help="project to download; can accept multiple values")
    parser.add_argument('-t', '--type', choices=['b','f','bam','fastq'], default='f', help='type of file to download')

    args = parser.parse_args()
    myAPI = BaseSpaceAPI(profile=args.profile, timeout=500)
    user = myAPI.getUserById('current')
    qp = QueryParameters.QueryParameters({'Limit':1024})

    projects = user.getProjects(myAPI, qp)
    
    if args.type in ['b', 'bam']:
        download = downloadProjectBam
    elif args.type in ['f', 'fastq']:
        download = downloadProjectFastq
      
    userProjs = stringsToBSObj(projects, args.project)
    for lostProj in set(args.project) - set([str(x) for x in userProjs]):
        warning("cannot find " + str(lostProj))
    TotalSize = 0
    for project in userProjs:
        TotalSize += download(project , myAPI, args.dry, force=args.force)
    if len(userProjs) > 1:
            print(humanFormat(totalSize) + "\tTotal")
        
if __name__ == "__main__":
    main()
    
        