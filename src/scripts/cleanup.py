from __future__ import print_function
from pdb import set_trace as stop
import pandas as pd
import os
import argparse
import logging
logging.basicConfig()

#BaseSpace API imports
from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI
from BaseSpacePy.api.BaseSpaceAPI import QueryParameters

def pathFromFile(fn, myAPI):
    '''
    take a basespace file object and turn it into an appropriate path 
    '''
    url = fn.getFileS3metadata(myAPI)['url']
    savePath = url.split('amazonaws.com/')[1].split('?AWSA')[0]
    savePath = "/".join(savePath.split('/')[1:-1])
    return savePath

def downloadRun(run, myAPI):
    '''
    uncomment the bits about makedirs and downloadFile to "arm" the download
    '''
    page = 0
    pageFiles = run.getFiles(myAPI, QueryParameters.QueryParameters({'Limit':1024, 'Offset':int(1024*page)}))
    while len(pageFiles) >0:
        for fn in pageFiles:
            #if fn.startswith('s_') and (fn.endswith('.clocs') or fn.endswith('.bcl.gz')):
            savePath = str(run) + "/" + pathFromFile(fn, myAPI)
            if not os.path.exists(os.path.split(savePath)[0]):
                #pass
                os.makedirs(os.path.split(savePath)[0])
            print(savePath)
            fn.downloadFile(myAPI, savePath)
        page += 1
        pageFiles = run.getFiles(myAPI, QueryParameters.QueryParameters({'Limit':1024, 'Offset':int(1024*page)}))

def downloadProjectFastq(project, myAPI, dryRun, samples=[], force=False, qp=QueryParameters.QueryParameters({'Limit':1024})):
    '''
    uncomment the bits about makedirs and downloadFile to "arm" the download
    '''
    totalSize = 0
    if not samples:
        samples = project.getSamples(myAPI, qp)
    for sample in samples:
        fns = sample.getFiles(myAPI, qp)
        for fn in fns:
            totalSize += fn.__dict__['Size']
            if dryRun:
                continue
            savePath = str(project).replace(" ","_") + "/" + pathFromFile(fn, myAPI)
            if not os.path.exists(savePath):
                os.makedirs(savePath)
            while force and os.path.exists(savePath + fn.Name):
                # if the path exists, append this string to the end to avoid overwriting
                counter = 1
                fn.Name = os.path.basename(fn.Path) + "." + str(counter)
                counter += 1 
            if not force and os.path.exists(savePath + fn.Name):
                print("already have " + savePath + fn.Name + ". Skipping...")
                continue
            else:
                print(os.path.join(savePath, fn.Name))
                fn.downloadFile(myAPI, savePath)
    print( humanFormat(totalSize) + '\t' + str(project) )
    return totalSize


def downloadProjectBam(project, myAPI, dryRun, samples=[], force=False, qp=QueryParameters.QueryParameters({'Limit':1024})):
    totalSize = 0
    results = project.getAppResults(myAPI, qp)
    for result in results:
        bams = [ x for x in result.getFiles(myAPI, qp) if "bam" in str(x) ]
        if samples:
            # user picked particular samples
            # subset the list of bams accordingly
            #bams = [x for x in bams if ]
            #WIP
            print("\n\nuser picked particular samples, but this isn't coded in yet\n")
            stop()
        for fn in bams:
            totalSize += fn.__dict__['Size']
            if dryRun:
                continue
            savePath = str(project).replace(" ","_") + "/" + pathFromFile(fn, myAPI)
            if not os.path.exists(savePath):
                os.makedirs(savePath)
            while force and os.path.exists(savePath + fn.Name):
                # if the path exists, append this string to the end to avoid overwriting
                counter = 1
                fn.Name = os.path.basename(fn.Path) + "." + str(counter)
                counter += 1 
            print(os.path.join(savePath, fn.Name))
            if not force and os.path.exists(savePath + fn.Name):
                print("already have " + savePath + "/" + fn.Name + ". Skipping...")
                continue
            else:
                fn.downloadFile(myAPI, savePath)
    print( humanFormat(totalSize) + '\t' + str(project) )
    return totalSize

def writeSummaryExcel(runs, projects):
    # write summary excel for pearlly
    runsDF = pd.DataFrame(index= [str(x) for x in pd.Index(runs)] )
    projDF = pd.DataFrame(index= [str(x) for x in pd.Index(projects)] )

    writer = pd.ExcelWriter('basespace_summary.xlsx')
    runsDF.to_excel(writer, 'Runs')
    projDF.to_excel(writer, 'Projects')

    for project in projects:
        _df = pd.DataFrame(index=pd.Index( [str(x) for x in project.getSamples(myAPI, qp)] ))
        _df.to_excel(writer, str(project).replace('/','').replace('\\','')[:30] )
    writer.save()

def downloadSampleSheet(run, myAPI):
    '''
    download a sample sheet for a run 
    '''
    page = 0
    #stop()
    pageFiles = run.getFiles(myAPI, QueryParameters.QueryParameters({'Limit':10, 'Offset':int(10*page)}))
    while len(pageFiles) >0:
        for fn in pageFiles:
            if str(fn)=="SampleSheet.csv":
                savePath = str(run) + "/" + pathFromFile(fn, myAPI)
                if not os.path.exists(os.path.split(savePath)[0]):
                    os.makedirs(os.path.split(savePath)[0])
                print(savePath)
                fn.downloadFile(myAPI, savePath)
                return True
        page += 1
        pageFiles = run.getFiles(myAPI, QueryParameters.QueryParameters({'Limit':1024, 'Offset':int(1024*page)}))

def getProjFilesFromMissingRuns():
    runsDF = pd.DataFrame(columns=['name','run'])
    projDF = pd.DataFrame(columns=['name','proj'])

    runsD = { 'name':{}, 'run':{}  }
    projD = { 'name':{}, 'proj':{} }
    count = 0
    for run in runs:
        status = run.__dict__['Status'] 
        if status == 'Complete':
            runsD['name'][count] = str()
            runsD['run'][count] = str(run)

def humanFormat(num):
    if int(num/(1<<40)):
        return "{0:.2f}".format(float(num)/(1<<40)) + "TB"
    if int(num/(1<<30)):
        return "{0:.2f}".format(float(num)/(1<<30)) + "GB"
    if int(num/(1<<20)):
        return "{0:.2f}".format(float(num)/(1<<20)) + "MB"
    if int(num/(1<<10)):
        return "{0:.2f}".format(float(num)/(1<<10)) + "KB"
    else:
        return num

def getFastqFromSampleList(project, samples, myAPI, qp):
    for sample in samples:
        fns = sample.getFiles(myAPI, qp)
        for fn in fns:
            savePath = str(project) + "/" + pathFromFile(fn, myAPI)
            if not os.path.exists(savePath):
                os.makedirs(savePath)
            while os.path.exists(savePath + fn.Name):
                # if the path exists, append this string to the end to avoid overwriting
                counter = 1
                fn.Name = os.path.basename(fn.Path) + "." + str(counter)
                counter += 1 
            print(os.path.join(savePath, fn.Name))
            fn.downloadFile(myAPI, savePath)

def pickSomething(selectionType, potentialSelectionsList):
    itemDict = {}
    for i, item in enumerate(potentialSelectionsList):
        itemDict[i] = item 
    for i,item in itemDict.items():
        print("[{0}]\t{1}".format(i, item))
    selection = raw_input("select a " + selectionType + " : ")
    if selection == "":
        # no selection assumes all selected
        outList = itemDict.items()
    else:
        outList = []
        for picked in selection.split():
            if int(picked) not in itemDict.keys():
                print("no " +selectionType+" with id #" + str(picked))
                continue
            outList.append(itemDict[int(picked)])
    return outList


def CLI(myAPI, inProjects, runs, dryRun, force):
    '''
    command line interface with the program
    allow user to select runs, projects, and/or fastq/bam files
    '''
    
    #print("[p]\tprojects".format(1))
    #print("[r]\truns".format(2))
    TotalSize = 0
    qp = QueryParameters.QueryParameters({'Limit':1024})
    scope = raw_input("select projects [p], samples [s], or runs [r]: " )
    while scope not in ['p', 'r', 's']:
        print("invalid selection")
        scope = raw_input("select projects [p], samples [s], or runs [r]: ")
    if scope == 'p' or scope == 's':
        projects = pickSomething("project(s)", inProjects)
        '''
        projDict = {}
        for i, project in enumerate(projects):
            projDict[i] = project 
        for i,project in projDict.items():
            #stop()
            print("[{0}]\t{1}".format(i, project))
        selection = int(raw_input("select a project: "))
        '''

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
                    TotalSize += downloadProjectBam(project, myAPI, dryRun, force=force, samples=samples)
        if len(projects) > 1:
            print(humanFormat(TotalSize) + "\tTotal")


    if scope == 'r':
        #todo
        pass
    finished = raw_input("finished? (y/n): ")
    while finished not in ['y', 'n']:
            print("invalid selection")
            finished = raw_input("finished? (y/n): ")
    if finished == "n":
        CLI(myAPI, inProjects, runs, dryRun, force)


    
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--profile', default="DEFAULT", help="the .basespacepy.cfg profile to load")
    parser.add_argument('-d', '--dry', action='store_true', default=False, help="dry run; return size of selected items")
    parser.add_argument('-f', '--force', action='store_true', default=False, help="force overwrite; otherwise cat counters on new filenames")
    args = parser.parse_args()
    myAPI = BaseSpaceAPI(profile=args.profile)
    user = myAPI.getUserById('current')
    qp = QueryParameters.QueryParameters({'Limit':1024})

    projects = user.getProjects(myAPI, qp)
    runs = user.getRuns(myAPI, qp)
    CLI(myAPI, projects, runs, args.dry, args.force )
    exit()
    '''
    # save all sample sheets associated with missing runs
    #   where the missing runs were identified outside of this script
    missing = []
    for line in open("missing","r"): 
        name = line.strip()
        missing.append([x for x in runs if str(x)==name][0])
    for run in missing:
        downloadSampleSheet(run, myAPI)
    '''
    downloadProjectBam(projects[-1],myAPI, qp)
    stop()
    pearllyEdit = '/home/OSUMC.EDU/krol06/Documents/Keepers_pearlly_full_proj_list.xlsx'
    res = [str(x) for x in pd.read_excel(pearllyEdit,header=None,index_col=0).index]
    keepers = []
    for item in res:
        for proj in projects:
            if str(proj) == item:
                keepers.append(proj)
                break
    TotTot = 0
    interest = keepers
    #interest = projects
    for project in interest:
        projTot = 0
        for sample in project.getSamples(myAPI, qp):
            #print(str(sample))
            projTot += sample.__dict__['TotalSize']
        TotTot += projTot
        print("{0}\t{1}".format(humanFormat(projTot), str(project)))
    print("{0}".format(humanFormat(TotTot)))
    
    

    #downloadProject(projects[-1], myAPI)

if __name__ == "__main__":
    main()
