#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 22 19:04:36 2020

reminder how to import packages form ipython console:
import pip
pip.main(['install', 'package_name'])

@author: Jackson Perry


this script will take a file locaiton such as a disk and copy every unique file to teh desitination updating the file name to represent the old path
 it will only process selected file types
 it will skip pdfs that are DOD fillable forms and LOGSA publications( technical manuals)
 
"""
 
import os
import multiprocessing
import hashlib
import time
import PyPDF4

'''
make a dictonary {path:size} for every file in the file tree
'''
def groupSize(path):
    sizeDict = {} # {path:size}
    for dirName, subdirs, fileList in os.walk(path):
        for filename in fileList:
            try:
                fullName = os.path.join(dirName, filename)
                file_size =os.path.getsize(fullName)
                if file_size > 4500:
                    sizeDict.update({fullName: file_size})
            except:
                continue
    by_size= {}
    for key, value in sizeDict.items():
        by_size.setdefault(value, set()).add(key)
    return by_size
''''
nake a dictionary {path: file extension} for every file in file tree THIS FUNCTION IS UNUSED
'''
def groupType(path):
    typeDict ={} #{path:.ext}
    for dirName, subdirs, fileList in os.walk(path):
        for filename in fileList:
            try:
                fullName = os.path.join(dirName, filename)
                file_ext= os.path.splitext(fullName)[1]
                typeDict.update({fullName: file_ext})
            except:
                continue
    by_type={}
    for key, value in typeDict.itmes():
        by_type.setdefault(value, set()).add(key)
    return by_type
'''
takes in a list of file sizes and compares them to a dictionary {size:path} hasehs and compares all files of the same size
'''
def Dupes(size_list, size_dict,out_q,owner):
    dups={} #{hash:path to file}
    n=1
    for size in size_list:
        v=n/len(size_list)
        if n%100 ==0:  
          print(f'CORE {owner}...{v:.2%} complete', end='\n')
        elif n==len(size_list):
            print(f'CORE {owner}...{v:.2%} complete', end='\n')
        n+=1
        for file in size_dict[size]:
            if len(size_dict[size]) > 1:
                file_hash=hashfile(file)
                if file_hash in dups:
                    dups[file_hash].append(file)
                else:
                    dups[file_hash] = [file]
       # print('size %s complete..' % size)
    out_q.put(dups)
    
'''                        
SHA1 hash of a file
'''
 
def hashfile(path, blocksize = 65536):
    afile = open(path, 'rb')
    hasher = hashlib.sha1()
    buf = afile.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(blocksize)
    afile.close()
    return hasher.hexdigest()
'''
takes a dictionary {{hash:pathtofile} and creates a txt file in parentfolder that lists all duplicate files also returns a list of all duplicates 
except the original 
 '''
def printResults(dict1, out_path, path):
    outFile=open(out_path+'/duplicateList.txt','w',encoding='utf-8')
    outFile2=open(out_path+'/allX.txt','w',encoding='utf-8')
    results = list(filter(lambda x: len(x) > 1, dict1.values()))
    all_files=list(groupSize(path).values())
    if len(results) > 0:
        outFile.write('Duplicates Found:\n')
        outFile.write('The following files are identical. The name could differ, but the content is identical\n')
        outFile.write('___________________\n')
        for result in results:
            counter =0
            for subresult in result:
                counter = counter+1
                try:
                    outFile.write(subresult)
                except:
                    continue
            outFile.write('___________________\n')
    else:
        outFile.write('No duplicate files found.')
    for i in all_files:
        outFile2.write('%s\n' % i)
    outFile2.close()
    final=[]
    for i in dict1.values():
       final.append(i[0])
    return final
'''
take number of cores to be used for hashing and reurns a dict {corenumber:[list of sizes]}
'''
def workChunks(cores,size_list):
    dict={}
    for i in range(cores):
        dict[i]=size_list[i::cores+1]
    return dict

'''
takes in path and number of cores on machine divides the hashing and comparing into chucks and processes them retuning a dict
'''

def multiCore(path,cores):
    size_dict= groupSize(path)
    size_list=list(size_dict.keys())
    size_list.sort(reverse=True)
    out_q= multiprocessing.Queue()
    procs=[]
    chunks=workChunks(cores,size_list) 
    start_time=time.time()
    for i in range(cores):
        p= multiprocessing.Process(target=Dupes, args=(chunks[i], size_dict, out_q,i))
        procs.append(p)
        print('core number %s starting...%s chunk size' % (i, len(chunks[i])))
        p.start()
    resultDict={}
    for i in range(cores):
        resultDict.update(out_q.get())
    for p in procs:
        print('core number %s  joining reuslt...%s' % (p, time.time()-start_time))
        p.join()
    return resultDict
'''
copies the unique files form /CLONE/ to /AWG/ This is too slow for big files use rsynce from linux terminal
'''

def CopyUnique(final,out_path,PATH):
    new=[]
    for file in final:
        file = file[len(PATH):]
        new.append(file.replace('/','_'))          
    n=0
    for i in new: 
        os.system('cp "%s" "%s"' % (final[n], out_path+new[n]))
        n+=1

'''
filters the list by file extension 
if it is a pdf file it checks to see if it is a govt form by checking for an OMB number in the metadata and skips it
if it is a pdf. file it cheks to see if it is a TM or other LOGSA pub and skips it
'''

def FilterbyType(final,docs):
    extension = docs
    docfiles=[]
    for i in extension:
        for lines in final: 
           if lines.find('.')!= -1:
              if lines.rsplit(sep='.', maxsplit=1)[1] ==i:
                  docfiles.append(lines)                    
    return docfiles

'''
Pass this function a list of pdf file full paths it will check the metadata of each and remove those that meet the conditions below
author is APD or LOGSA, pdf printed form a website of iphone, any copies of the GPC request form.
'''
def PDFgetMeta(x):
    docfiles={}
    for i in x:
        doc=PyPDF4.PdfFileReader(i,strict=False)
        docfiles.update({i:doc})
    return docfiles
 
def FilterPDF(docfiles):
    x=docfiles.keys()
    for i in x:
        if docfiles[x[i]].getDocumentInfo['/Author'] == 'APD' or '(LOGSA)':
            del [x[i]]
        elif docfiles[x[i]].getDocumentInfo['/Pruducer'] == 'HP Digital Sending Device' or 'Microsoft: Print To PDF' or'iText 2.1.2 (by loagie.com)':
            del [x[i]]
        elif docfiles[x[i]].getDocumentInfo['/Title'] == 'AWG GPC Request Form':
            del [x[i]]
    return x


def MultiPdfFilter(final, cores):
    out_q= multiprocessing.Queue()
    procs=[]
    start_time=time.time()
    for i in range(cores):
        p= multiprocessing.Process(target=PDFgetMeta, args=(list(workChunks(cores,final)[i])))
        procs.append(p)
        print('core number %s starting...%s chunk size' % (i, len(workChunks(cores,final)[i])))
        p.start() 
    results=[]
    for i in range(cores):
        results.append(out_q.get())
    for p in procs:
        print('core number %s  joining reuslt...%s' % (p, time.time()-start_time))
        p.join()




def Multicopy(out_path,cores,final,path):
    procs=[]
    chunks=workChunks(cores,final)
    for i in range(cores):
        p= multiprocessing.Process(target=CopyUnique, args=(chunks[i], out_path,path))
        procs.append(p)
        print('core number %s starting...%s chunk size' % (i, len(chunks[i])))
        p.start()       
   
if __name__ == "__main__":
    PATH='/mnt/CLONE/'  
    OUT_PATH='/mnt/AWG/'
    CORES= 12
    documents= ['pdf','PDF','docx','DOCX','doc','DOC','ppt','PPT','pptx','PPTX','xlsx','XLSX','xls','XLS','txt','TXT','rtf,','RTF','msg','MSG','pst','PST','zip']
    pictures= ['MOV','mov','JPG','jpg','png','PNG','jpeg','JPEG','tif','TIF','tiff','TIFF','mp3','MP3','mp4','MP4','gif','GIF']
    #put the list of file extentions below commons lists provided above lists are concatenate in with + to add more files of type .new add +['new','NEW']
    DOCS= documents
    
    
  
    RESULT = multiCore(PATH, CORES) 
    FINAL=printResults(RESULT,OUT_PATH,PATH)
    FILTERED=FilterbyType(FINAL,DOCS)
    Multicopy(OUT_PATH,CORES,FILTERED,PATH)






