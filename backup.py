import pymysql
import argparse
import os, sys, os.path
import datetime
import ftputil
import zipfile
import codecs
import shutil

'''
First we process the command line arguments.
Secondly we fetch all tables in the database, get their structure and dump their rows along
with this information.
Thirdly we zip it all up and label it in the format YYYY-mm-dd_HH:MM.

====================================
END OF PROGRAM STRUCTURE DESCRIPTION
       HERE BE DRAGONS
====================================

                                                  .~))>>
                                                 .~)>>
                                               .~))))>>>
                                             .~))>>             ___
                                           .~))>>)))>>      .-~))>>
                                         .~)))))>>       .-~))>>)>
                                       .~)))>>))))>>  .-~)>>)>
                   )                 .~))>>))))>>  .-~)))))>>)>
                ( )@@*)             //)>))))))  .-~))))>>)>
              ).@(@@               //))>>))) .-~))>>)))))>>)>
            (( @.@).              //))))) .-~)>>)))))>>)>
          ))  )@@*.@@ )          //)>))) //))))))>>))))>>)>
       ((  ((@@@.@@             |/))))) //)))))>>)))>>)>
      )) @@*. )@@ )   (\_(\-\b  |))>)) //)))>>)))))))>>)>
    (( @@@(.@(@ .    _/`-`  ~|b |>))) //)>>)))))))>>)>
     )* @@@ )@*     (@)  (@) /\b|))) //))))))>>))))>>
   (( @. )@( @ .   _/  /    /  \b)) //))>>)))))>>>_._
    )@@ (@@*)@@.  (6///6)- / ^  \b)//))))))>>)))>>   ~~-.
 ( @jgs@@. @@@.*@_ VvvvvV//  ^  \b/)>>))))>>      _.     `bb
  ((@@ @@@*.(@@ . - | o |' \ (  ^   \b)))>>        .'       b`,
   ((@@).*@@ )@ )   \^^^/  ((   ^  ~)_        \  /           b `,
     (@@. (@@ ).     `-'   (((   ^    `\ \ \ \ \|             b  `.
       (*.@*              / ((((        \| | |  \       .       b `.
                         / / (((((  \    \ /  _.-~\     Y,      b  ;
                        / / / (((((( \    \.-~   _.`" _.-~`,    b  ;
                       /   /   `(((((()    )    (((((~      `,  b  ;
                     _/  _/      `"""/   /'                  ; b   ;
                 _.-~_.-~           /  /'                _.'~bb _.'
               ((((~~              / /'              _.'~bb.--~
                                  ((((          __.-~bb.-~
                                              .'  b .~~
                                              :bb ,'
                                              ~~~~
'''

#
# Setup arguments
#
parser = argparse.ArgumentParser()
parser = argparse.ArgumentParser(description='Backup files from a MySQL database and FTP. Zip it all up in a neat, dated file.')

parser.add_argument('-mh','--mysql-host', help='The host of the MySQL database', required=True)
parser.add_argument('-mu','--mysql-user', help='The user of the MySQL database', required=True)
parser.add_argument('-mp','--mysql-pass', help='The password for the user of the MySQL database', required=True)
parser.add_argument('-mdb','--mysql-database', help='The database name on the MySQL host', required=True)
parser.add_argument('-fh','--ftp-host', help='The FTP host', required=True)
parser.add_argument('-fu','--ftp-user', help='The FTP user', required=True)
parser.add_argument('-fp','--ftp-pass', help='The FTP password', required=True)
parser.add_argument('-fd','--ftp-dir', help='The path on FTP to download from', required=False)
parser.add_argument('-o','--output', help='The output file path', required=True)
args = vars(parser.parse_args())

#
# Determine output directory
#
now = datetime.datetime.now()
out = args['output'] + '/' + now.strftime("%Y-%m-%d_%H_%M")

#
# Dump SQL
#
print('Connecting to MySQL.')

con = pymysql.connect(host=args['mysql_host'], user=args['mysql_user'], passwd=args['mysql_pass'], db=args['mysql_database'])
cur = con.cursor()

# Get all tables
print('Getting MySQL tables.')

cur.execute('SHOW TABLES')
data = ''
tables = []
for table in cur.fetchall():
    tables.append(table[0])

# Loop over all tables and dump the description of it
for table in tables:
    print('Getting table ' + table)
    data += 'DROP TABLE IF EXISTS `' + str(table) + '`;'

    cur.execute('SHOW CREATE TABLE `' + str(table) + '`;')
    data += "\n" + str(cur.fetchone()[1]) + ";\n\n"

    cur.execute("SELECT * FROM `" + str(table) + "`;")

    # Dump all rows
    print('Getting table rows for ' + table)
    i = 0
    for row in cur.fetchall():
        data += "INSERT INTO `" + str(table) + "` VALUES("
        first = True
        for field in row:
            if not first:
                data += ', '
            data += '"' + str(field) + '"'
            first = False
        data += ');\n'
        i = i + 1
    data += '\n\n'
    print('Got ' + str(i) + ' rows.')

# Write it to a file
print('Dumping the database to a file.')
filename = out + '/database.sql'
if not os.path.exists(os.path.dirname(filename)):
    os.makedirs(os.path.dirname(filename))
with open(filename, 'w') as dump:
    dump.writelines(data)
    dump.close()

print('Done dumping database.')

#
# Dump FTP
#
print('Logging in to FTP.')
host = ftputil.FTPHost(args['ftp_host'], args['ftp_user'], args['ftp_pass'])

dir = '.'
if args['ftp_dir']:
    dir = args['ftp_dir']

dumpftp = True
if not dir == '.':
    print('Changing directory to ' + dir)
    try:
        host.chdir(dir)
    except OSError:
        print('Directory ' + dir + ' does not exist on the FTP.')
        print('Stopping FTP download.')
        dumpftp = False

if dumpftp:
    failedFiles = []
    for curdir, subdirs, files in host.walk(host.curdir):
        for fname in files:
            # .htaccess oddness
            if fname == '.htaccess':
                continue
            dest = out + '/' + curdir + '/' + fname
            src = curdir + '/' + fname
            if not os.path.exists(os.path.dirname(dest)):
                os.makedirs(os.path.dirname(dest))
            print('Downloading ' + src  + ' to ' + dest)
            host.keep_alive()
            try:
                host.download(src, dest)
            except ftputil.error.FTPIOError:
                print('Downloading ' + src + ' failed. Trying later.')
                failedFiles.append((src, dest))

    print('Trying to redownload failed files')
    tries = 1
    while tries <= 3 and len(failedFiles) > 0:
        print('Attempt ' + str(tries))
        for src, dest in failedFiles:
            try:
                host.keep_alive()
                host.download(src, dest)
                failedFiles.remove((src, dest))
            except ftputil.error.FTPIOError:
                print('Downloading ' + src + ' failed. Trying later.')
                failedFiles.append((src, dest))
        tries = tries + 1
    print('Done dumping FTP.')

#
# Zip it all up
#
def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))

if __name__ == '__main__':
    print('Zipping up all the things.')
    zipf = zipfile.ZipFile(out + '.zip', 'w')
    zipdir(out, zipf)
    zipf.close()
    shutil.rmtree(out)
    print('All done.')
