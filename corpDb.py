#!/usr/bin/python
# -*- coding: UTF-8 -*-
import MySQLdb as mdb
import os
import argparse 

try:
    dbconn = mdb.connect(host="127.0.0.1", port=3306, user="miner", passwd="suyu", charset='utf8')
    dbconn.select_db("Corps")
    dbcursor = dbconn.cursor()
except Exception as e:
        print str(e)
        
def scan_files(directory,prefix=None,postfix=None):
    files_list=[]
    
    for root, sub_dirs, files in os.walk(directory):
        for special_file in files:
            if postfix:
                if special_file.endswith(postfix):
                    files_list.append(os.path.join(root,special_file))
            elif prefix:
                if special_file.startswith(prefix):
                    files_list.append(os.path.join(root,special_file))
            else:
                files_list.append(os.path.join(root,special_file))
                          
    return files_list
    
def write_data_to_db(project,file_path):
    #判断文件是否存在
    if not os.path.exists(file_path):
        return "file: %s not exist" % file_path
        
    with open(file_path, 'rb') as file:
        fileData = file.read()
    
    (path, file_name) = os.path.split(file_path)
    #生成sql语句
    sql = "INSERT INTO Corps(project,filename,corp) VALUES " "('%s','%s','%s')" % \
          (project, file_name, mdb.escape_string(fileData))
 
    try:           
        dbcursor.execute(sql)
        dbconn.commit()
    except Exception as e:
        print str(e)
        return str(e)
    return "insert success"

def wite_data_to_file(project,file_path):
    #判断文件是否存在
    if not os.path.exists(file_path):
        return "path: %s not exist" % file_path
    sql = "SELECT filename , corp FROM Corps where project = '%s' " % project
    print(sql)
    
    try:        
        count = dbcursor.execute(sql)
        print count
        for i in range(1,count):            
            filename,corp = dbcursor.fetchone()        
            print i ,"/",count, "==>", filename            
            file=open(os.path.join(file_path,filename),"wb")
            file.write(corp)
            file.close()
    except Exception as e:
        print str(e)
        return str(e)
 
def query_all():
    sql = "SELECT project , count(1) as summary FROM Corps GROUP BY project "
    print(sql)
    
    try:        
        count = dbcursor.execute(sql)
        rs = dbcursor.fetchall()
        for r in rs:
            print r
    except Exception as e:
        print str(e)
        return str(e)
        
def query_project(project):
    sql = "SELECT id,project filename, time FROM Corps WHERE project = '%s'" % project
    print(sql)
    
    try:        
        count = dbcursor.execute(sql)
        rs = dbcursor.fetchall()
        for r in rs:
            print r
    except Exception as e:
        print str(e)
        return str(e)        
        
        
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-u","--upload",help="import corp files from db",action="store_true")    
    parser.add_argument("-d","--download",help="outport corp files from db",action="store_true")
    parser.add_argument("-q","--query",help="query corp summary from db, -n to query the special project ",action="store_true")
    parser.add_argument("-f","--file",type=str,required=False,help="import or outport a corp file")
    parser.add_argument("-p","--path",type=str,required=False,help="import or outport corp file from a path")
    parser.add_argument("-n","--name",type=str,required=False,help="project name of the corps")      
    args = parser.parse_args()
    
    if args.upload:            
        if not args.name:
            print "You SHOULD write -n argument "
            return
        if args.file:
            status = write_data_to_db(args.name,args.file)
            print(args.file,status)
        elif args.path:
            files_list = scan_files(args.path)
            for file in files_list:
                status = write_data_to_db(args.name,file)    
                print(file,status)
            
    if args.download:
        if not args.name:
            print "You SHOULD write -n argument "
            return    
        if args.path:
            wite_data_to_file(args.name,args.path)
        else:
            print("You MUST add -p argument to assign download path")
            
    if args.query:
        if args.name:
            query_project(args.name)
        else:
            query_all()           
    
    dbcursor.close()        
    dbconn.close()
                
if __name__ == "__main__":
    main()
