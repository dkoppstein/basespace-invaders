from __future__ import print_function
from pdb import set_trace as stop
import os
import argparse
import shutil 
import datetime
try:
    import pandas as pd
except ImportError:
    raise Warning("cannot parse metadata without Pandas for python") 
#BaseSpace API imports
from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI
from BaseSpacePy.api.BaseSpaceAPI import QueryParameters
#Aggregate of local utility functions
from Util import *

def downloadProjectMetadata(project, myAPI, samples=[], qp=QueryParameters.QueryParameters({'Limit':1024})):
    totalSize = 0
    sampleMetadata = pd.DataFrame()
    sindx = 0 
    fileMetadata = pd.DataFrame()
    findx = 0
    if not samples:
        samples = project.getSamples(myAPI, qp)
    elif samples and type(samples[0]) == str: 
        #convert samples strings to sample objects
        samples = stringsToBSObj(project.getSamples(myAPI, qp), samples)
    for sample in samples:
        sampleMetadata = sampleMetadata.append(pd.DataFrame(pullMetadata(sample),index=[sindx]))
        sindx += 1
        fns = sample.getFiles(myAPI, qp)
        for fn in fns:
            thisFileMeta = pd.DataFrame(pullMetadata(fn),index=[findx])
            thisFileMeta['SID'] = str(sample)
            fileMetadata = fileMetadata.append(thisFileMeta)
            findx += 1
    timestamp = str(datetime.datetime.today()).replace(' ','_') 
    savePath = str(project).replace(" ","_") + "/" + pathFromFile(fns[0], myAPI) 
    if not os.path.exists(savePath):
        os.makedirs(savePath)      
    sampleMetadata.to_csv( os.path.join(savePath, str(project) + '_SampleMetadata.'+ timestamp +'.txt'),sep='\t',header=True,index=False)
    fileMetadata.to_csv( os.path.join(savePath, str(project) + '_FileMetadata.'+ timestamp +'.txt'),sep='\t',header=True,index=False) 
    return sampleMetadata, fileMetadata 

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--profile', default="DEFAULT", help="the .basespacepy.cfg profile to load")    
    parser.add_argument('-j', '--project', required=True, nargs="+", help="project to download; can accept multiple values")

    args = parser.parse_args()
    myAPI = BaseSpaceAPI(profile=args.profile, timeout=500)
    user = myAPI.getUserById('current')
    qp = QueryParameters.QueryParameters({'Limit':1024})

    projects = user.getProjects(myAPI, qp)
    userProjs = stringsToBSObj(projects, args.project)
    for lostProj in set(args.project) - set([str(x) for x in userProjs]):
        warning("cannot find " + str(lostProj))
    
    fullSampleMetadata = pd.DataFrame()
    fullFileMetadata   = pd.DataFrame()
    for project in userProjs:
        smout, fmout = downloadProjectMetadata(project , myAPI)
        fullSampleMetadata = fullSampleMetadata.append(smout)
        fullFileMetadata   = fullFileMetadata.append(fmout)        
    thisInstant = str(datetime.datetime.today()).replace(' ',';')
    fullSampleMetadata.to_csv('fullSampleMetadata.'+thisInstant+'.txt',sep='\t',header=True,index=False)
    fullFileMetadata.to_csv('fullFileMetadata.'+thisInstant+'.txt',sep='\t',header=True,index=False)    
if __name__ == "__main__":
    main()    
