from __future__ import print_function
from pdb import set_trace as stop
import os
import argparse
#BaseSpace API imports
from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI
from BaseSpacePy.api.BaseSpaceAPI import QueryParameters
#Aggregate of local utility functions
from Util import *

def downloadRun(run, myAPI, dryRun, files=[], force=False):
    # you can only pull 1024 items at once, so we have to loop over "pages" of items, 1024 at a time
    # this is done by incrementing the offset by 1024 each time, so the next loop gets the next page
    # the limit can be adjusted as long as the limit is equal to the offset 
    page = 0
    pageFiles = run.getFiles(myAPI, QueryParameters.QueryParameters({'Limit':1024, 'Offset':int(1024*page)}))
    totalSize = 0
    #did the user select files?
    fileSel = False
    if files:
        fileSel = True  
    # todo: insert regex matching to pull down only those required for demultiplex
    while len(pageFiles) >0:
        for fn in pageFiles:
            if fileSel and fn.Name not in files:
                # user selected some particular files, but this aint one of em 
                continue
            elif files and fn.Name in files:
                # we found it! cut it from the list 
                files.pop(files.index(fn.Name))           
            thisSize = fn.__dict__['Size']
            totalSize += thisSize
            if dryRun:
                continue
            savePath = str(run) + "/" + pathFromFile(fn, myAPI)
            if not os.path.exists(savePath):
                os.makedirs(savePath)
            if not force and os.path.exists(os.path.join(savePath, fn.Name)):
                print("already have " + savePath + fn.Name + ". Skipping...")
                continue
            else:
                fn.downloadFile(myAPI, savePath)
        if fileSel and len(files) == 0:
            # user selected some file(s) and we found them all; return
            break
        page += 1
        pageFiles = run.getFiles(myAPI, QueryParameters.QueryParameters({'Limit':1024, 'Offset':int(1024*page)}))
    if files:
        # files was user-defined, but didn't successfully pop all elements
        # i.e. something was selected and never found
        print("warning: could not find these selected files")
        for fn in files:
            print('\t' + fn)
    print( humanFormat(totalSize) + '\t' + str(run) + '\t' + str(run.ExperimentName) ) 
    return totalSize
    
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--profile', default="DEFAULT", help="the .basespacepy.cfg profile to load")
    parser.add_argument('-d', '--dry', action='store_true', default=False, help="dry run; return size of selected items")
    parser.add_argument('-f', '--force', action='store_true', default=False, help="force overwrite; otherwise cat counters on new filenames")
    parser.add_argument('-r', '--run', required=True, nargs="+", help="run name to download; can accept multiple values")
    parser.add_argument('--file', default=[], nargs="+", help="specific file(s) to pull from each run; can accept multiple values")

    args = parser.parse_args()
    myAPI = BaseSpaceAPI(profile=args.profile, timeout=500)
    user = myAPI.getUserById('current')
    qp = QueryParameters.QueryParameters({'Limit':1024})

    runs = user.getRuns(myAPI, qp)
    userRuns = stringsToBSObj(runs, args.run)
    for lostRun in set(args.run) - set([str(x) for x in userRuns]):
        warning("cannot find " + str(lostRun))
    TotalSize = 0
    for run in userRuns:
        TotalSize += downloadRun(run, myAPI, args.dry, files=args.file, force=args.force)
    
if __name__=="__main__":
    main()