

# This is going to connect to database and run query on your behalf
def connect(inQuery):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        #establishing the connection
        conn = psycopg2.connect(
                database="demo_db", user='nick_usr', password='123demo123', host='localhost', port= '5432',
                cursor_factory=RealDictCursor)
		
        # create a cursor
        cur = conn.cursor()
        
	# execute a statement
        #print('PostgreSQL database version:')
        cur.execute(inQuery)

        # display the PostgreSQL database server version
        #db_version = cur.fetchone()
        return(cur.fetchall())
       
	# close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.commit()
            conn.close()
            print('Database connection closed.')


#Rframe = None # Global variable representing policy list

def resetInputField(text):
    inputField.delete(1.0, END)
    inputField.insert("1.0", text) 

def getViewPolicy(name):
    getView = "select pg_get_viewdef('%s', true)" % name
    cur = connect(getView)
    policyField.delete(1.0, END)
    policyField.insert("1.0", cur[0]['pg_get_viewdef'])
    
    
def populateFrame(data, tgtFrame, widthi = 12):
#    firstRow = True
    for i in range(len(data)): 
        #print("INPUT "+ str(data[i]))

        #if firstRow:
        #    for jk in data[i].keys():        
        #        e = Label(tgtFrame,width=15, text=str(data[i][j]), borderwidth=2,relief='ridge', anchor="w", font=('Arial', 10)) 
        #        e.grid(row=0, column=j) 

        for j in range(len(data[i])):
        #for jk in data[i].keys
            if type(data[i][j]) == type(memoryview(bytearray('XYZ', 'utf-8'))):
#                print(" POPU I "+ str(binascii.hexlify(bytearray(data[i][j]))))
                data[i][j]= str(binascii.hexlify(bytearray(data[i][j])))[20:40]
            
            el = Label(tgtFrame,width=widthi, text=str(data[i][j]), borderwidth=2,relief='ridge', anchor="w", font=('Arial', 10)) 
            if str(data[i][j]).startswith('dbc_policy'):
                el.bind("<Button-1>", lambda event: getViewPolicy(event.widget.cget("text")))               
            elif str(data[i][j]).startswith('dbc'): # That's the tables
                el.bind("<Button-1>", lambda event: resetInputField('SELECT * FROM '+event.widget.cget("text")))
                                
            el.grid(row=i+1, column=j) 
#        firstRow = False

    return None

# CREATE PURGE dbc_policy_vital_one_year AS
# SELECT bloodPressure, weight, time FROM dbc_vital
# WHERE DATE_PART(’day’, CURRENT_DATE - dbc_vital.time) > 365;

#       CREATE PURGE dbc_policy_patientpolicy AS
#SELECT fullname FROM dbc_patient
#WHERE dob > current_date - 30 * 365 * interval '1 day';


def runPolicy():
   query = policyField.get("1.0",END)

   #print ("QUERY " + query)
   if query.lower().strip().startswith('create purge '):
       query = query.replace('PURGE', 'VIEW') 
       #print(query)
       out = connect(query)  # Don't forget to repopulate!
       # Pull the policy and update the policyholder table
       pname = query.split('VIEW')[1].split()[0]  # name of the policy
       pname = pname.split('_')[2]  # real name of the real policy
       
       duration = int(query.split('-')[-1].split('*')[0]) * int(query.split('-')[-1].split('*')[1])
       policyQuery1 = "UPDATE policyholder SET purgingduration = %s WHERE policyname = '%s'" % (str(duration), pname)
       #print(" UPDATE POLICY " + policyQuery1)
       connect(policyQuery1)

#   elif query.lower().strip().startswith('create purg')
   elif query.lower().strip().startswith('drop policy'):
       query = query.replace('POLICY', 'VIEW')
#       print (" QUERY NOW " + query)
       out = connect(query)
#       print (" QUERY RESULT " + str(out))
   for widget in Rframe.winfo_children():
       widget.destroy()
#       Rframe.grid_remove()
#       Rframe.grid(row=2, column=0)

       # I hope this includes the views. It seems to initially. 
   allTables = connect("select * from information_schema.tables")
#       print(' AL TABLES ' + str(allTables))
   populatePolicy(allTables, Rframe)

def runQuery():
   query = inputField.get("1.0",END)

   out = connect(query)
#   print (" OUT OUT " + str(out))

   # Wipe out everything regardless
   for widget in outputGrid.winfo_children():
       widget.destroy()
   outputGrid.grid_remove()
   outputGrid.grid(row=2, column=0)

   if out: #  and len(out) <= 0:
     #  return None # Do nothing, empty query result

     headers = list(map(lambda x:x.upper(), out[0].keys()))
     out2 = [headers]
#     if len(out) == 0:
         
     for row in out[:30]:
#         print (" ROW "+str(row))
#         print (" ROWROW "+str(list(map(lambda x:x[1], row))))
         rowadd=[]
         for key in headers:
             rowadd.append(row[key.lower()])
         
         out2.append(rowadd) #list(map(lambda x:x[1], row)))
#     populateFrame(out[:10], outputGrid)
     populateFrame(out2, outputGrid)

def populatePolicy(allTables, Rframe):
    take4 = []
    for i in allTables[:]: # Take the first 5 tables. Fix later to relevant tables
#        print(" II IS " + str(i))  #str(i['table_name']))
        if i['table_name'].startswith('dbc_policy'):
            take4.append([i['table_name']]) # Table name is 3rd in output
    #print(" TAKE4 " + str(take4))
    populateFrame(take4, Rframe, 22)

def exportTables(tblList):
    
    for tbl in tblList:
        rows = connect("SELECT patientid, fullname::VARCHAR, fullname_keyid, dob, dob_keyid FROM " +tbl)

#        print('rows '+ str(rows))        
        # Write rows to CSV
        with open(tbl + ".csv", "w") as f:
            for row in rows:
 #               first = True
#                for rowval in row.values():
#                    if not first:
#                        f.write(bytes(',', encoding='utf8'))
#                    if first:
#                        first = False

#                    print(" TYPE OF ROWVAL " + str(type(rowval))+ " AND TYPE OF BYTES "+str(type(memoryview(bytes('', encoding='utf8')))))
#                    print("STR " + str(rowval))
#                    if type(rowval) == type(memoryview(bytes('', encoding='utf8'))):
#                        f.write(rowval.tobytes())
#                    else:
#                        f.write(bytes(str(rowval), encoding='utf8'))
                #f.write(rows[row])
                f.write(",".join([str(cell) for cell in row.values()]) + "\n")
#                f.write(bytes('\n', encoding='utf8'))

#restoreQuery = """SELECT PGP_SYM_DECRYPT(FULLNAME, Encryptionkey), DOB, EXPERATIONDATE, Encryptionkey
#FROM %s, encryptionoverview 
#WHERE policyid = FULLNAME_keyid AND
#experationdate <= '%s'::date"""

#"""UPDATE dbc_patient SET FULLNAME = (SELECT PGP_SYM_DECRYPT(FULLNAME, Encryptionkey) FROM encryptionoverview WHERE policyid = FULLNAME_keyid AND experationdate <= '%s'::date"""

restoreQuery = """UPDATE %s SET FULLNAME = (SELECT PGP_SYM_DECRYPT(FULLNAME, Encryptionkey) FROM %s, encryptionoverview  WHERE encryptionoverview.policyid = FULLNAME_keyid AND dbc_patient.patientid = dbc_patient_shadow.patientid AND experationdate > '%s'::date) WHERE EXISTS (SELECT PGP_SYM_DECRYPT(FULLNAME, Encryptionkey) FROM %s, encryptionoverview  WHERE encryptionoverview.policyid = FULLNAME_keyid AND dbc_patient.patientid = dbc_patient_shadow.patientid AND experationdate > '%s'::date)"""
purgeQuery = """UPDATE %s SET FULLNAME = (SELECT NULL FROM %s, encryptionoverview  WHERE encryptionoverview.policyid = FULLNAME_keyid AND dbc_patient.patientid = dbc_patient_shadow.patientid AND experationdate <= '%s'::date) WHERE EXISTS (SELECT NULL FROM %s, encryptionoverview  WHERE encryptionoverview.policyid = FULLNAME_keyid AND dbc_patient.patientid = dbc_patient_shadow.patientid AND experationdate <= '%s'::date)"""

def restoreTables(tblList, date1):
    date1 = date1.strip()
    for tbl in tblList:
        basetbl = tbl.replace('_shadow', '')
        #out = connect('TRUNCATE %s CASCADE' % basetbl) # Clean the table out

        print(" REST " + restoreQuery % (basetbl, tbl, date1, tbl, date1))
        # I want this query to do update with subquery based on each date option. 
        out = connect( restoreQuery % (basetbl, tbl, date1, tbl, date1))  # good to go.
        out = connect( purgeQuery % (basetbl, tbl, date1, tbl, date1))  # good to go.
        #expiredIDs = []
        #for row in out: # These are expired lists
        #    expiredIDs.append(row[])
        #print('UPDATE %s SET FULLNAME = NULL' % basetbl)
        #connect('UPDATE %s SET FULLNAME = NULL' % basetbl)
#        print(" OUT IS " + str(out))
        # Select things of right date. 
    
#1, Imports the tkinter module.
import tkinter
from tkinter import *
import tkinter.font as font
import psycopg2
from psycopg2.extras import RealDictCursor
import binascii

allTables = connect('select * from information_schema.tables')
#print(allTables)

#2, Creates a main GUI window.
root = tkinter.Tk()
root.title('DBCompliant')
#root.iconbitmap('SSQLtips.ico')
# Outer window
root.geometry('800x900+100+100')
root.resizable(0,0)
root.config(bg='lightgrey')

topHeight=300
HeaderFont = ("Arial", 12)
ButtonFont = font.Font(family='Helvetica', size=12, weight=font.BOLD)


# Outer root for the top level of the box
#subRoot = tkinter.Frame(root, width=790, height=400, bg = 'yellow', padx=5, pady=5)
#subRoot.grid(row=0, column=0, padx=5, pady=5)

#3, Adds widgets to the window.
#3.1 Creates frames.
topFrame = LabelFrame(root)
topFrame.grid(row=0, column=0, padx=5, pady=5)
#3.2 Arranges the frames.
#greeting_frame.pack()
#3.3 Uses the greeting frame to organize widgets.
#3.3.1 Adds a label to the greeting frame.
#greeting_label = tkinter.Label(greeting_frame, text='Welcome to MSSQLTips')
#3.3.2 Arranges the label.
#greeting_label.pack()


# List of tables in the DB
WrapperLframe = tkinter.LabelFrame(topFrame, width=140, height=topHeight, bg='lightblue')
# ok? LLframe.pack(side=LEFT)
WrapperLframe.grid(row=0, column=0, padx=5, pady=5)
tl = Label(WrapperLframe, text="Database Tables", font=HeaderFont, padx=10)
tl.grid(row=0, column=0)
#tl.pack(side = TOP)

# The left window with table list
#tkinter.Label(LLframe, text="Database Tables").grid(row=0, column=0, padx=5, pady=5)

# The actual list of tables (label frame to work properly)
LLframe = tkinter.LabelFrame(WrapperLframe, width=120, height=topHeight, bg='lightgreen')
LLframe.grid(row=1, column=0, padx=5, pady=5)

take5 = []
for i in allTables[:]: # This is the table list on the left
#    print(" II IS " + str(i['table_name']))
    if i['table_name'].startswith('dbc_') and not i['table_name'].startswith('dbc_policy'):
        take5.append([i['table_name']]) # Table name is 3rd in output
#print(take5)
populateFrame(take5, LLframe, 14)

#The input query area which has Input and Button
t2 = Label(WrapperLframe, text="Query Input", font=HeaderFont)
t2.grid(row=0, column=1, pady =2 )

Lframe = tkinter.LabelFrame(WrapperLframe, width=420, height=topHeight, bg='lightgrey')
Lframe.grid(row=1, column=1, padx=5, pady=5)

#qInput = tkinter.Entry(Lframe, width =40)
#qInput.grid(row=0,column=0)
# qInput.focus_set()
# qInput.pack()
# The run button
runButton = tkinter.Button(Lframe, text='Run Query', font=ButtonFont, command = runQuery)
runButton.grid(row=1, column=0)

spacer1 = tkinter.Label(Lframe, text="", bg='lightgrey')
spacer1.grid(row=2, column=0)

backupButton = tkinter.Button(Lframe, text='Create Backup', font=ButtonFont, command=lambda: exportTables(['dbc_patient_shadow'])) # , command = runQuery)
backupButton.grid(row=3, column=0)

spacer2 = tkinter.Label(Lframe, text="", bg='lightgrey')
spacer2.grid(row=4, column=0)


restoreOption = tkinter.LabelFrame(Lframe, width=120, bg='lightgrey')
restoreOption.grid(column=0, row=5)

restoreButton = tkinter.Button(restoreOption, text='Restore Backup at: (mm/dd/yy):', font=ButtonFont, command = lambda: restoreTables(['dbc_patient_shadow'], restoreDateInput.get(1.0, END)))
restoreButton.grid(row=0, column=0)

restoreDateInput = tkinter.Text(restoreOption, width=10, height=1)
restoreDateInput.grid(row=0, column=1)


#runButton.pack(side='bottom')
# The input field for the query stuff.
inputField = tkinter.Text(Lframe, width=44, height=12)
inputField.grid(column=0, row=0)
#runButton2 = tkinter.Button(Bframe, text='Run Query')
#runButton2.grid(row=1, column=0)

# The r-side frame that contains policy input window and the run button of its own
# Shorten hight so it fits with left side
WrapperRframe = tkinter.LabelFrame(topFrame, width=200, height=topHeight, bg='lightblue')
WrapperRframe.grid(row=0, column=2, padx=5, pady=5)

t2 = Label(WrapperRframe, text="Policy Input", font=HeaderFont)  # The button
t2.grid(row=0, column=0, pady = 2)



policyField = tkinter.Text(WrapperRframe, width=32, height=10)
policyField.grid(row=1, column=0)

# An actual run policy button
runButton = tkinter.Button(WrapperRframe, text='Run Policy', font=ButtonFont, command = runPolicy)
runButton.grid(row=2, column=0)


# RANDOM STUFF
# The database policy label
tl = Label(WrapperRframe, text="Database Policies", font=HeaderFont, padx=10)
tl.grid(row=3, column=0, pady = (15, 4))


#runButton = tkinter.Button(WrapperRframe, text='Define Policy', font=ButtonFont)
#runButton.grid(row=3, column=0)

Rframe = tkinter.LabelFrame(WrapperRframe, width=200, height=topHeight/3, bg='lightgreen')
Rframe.grid(row=4, column=0, padx=5) #, pady=(0,30))


bottomFrame = LabelFrame(root, width=800, height=200, bg='lightblue')
bottomFrame.grid(row=1, column=0, padx=5, pady=5)

###take3 = [] # This should be the policies
###for i in allTables[14:18]: # Take the first 5 tables. Fix later to relevant tables
###    take3.append([i['table_name']]) # Table name is 3rd in output
####print(take5)
###populateFrame(take3, Rframe)

populatePolicy(allTables, Rframe)


tl = Label(bottomFrame, text="Query Output", font=HeaderFont)
tl.grid(row=0, column=0)

outputGrid = Frame(bottomFrame, width=780, height=480)
outputGrid.grid(row=2, column=0)

#for i in range(1, 6): 
#    for j in range(8):
#        e = Label(outputGrid,width=10, text=str(i)+'_'+str(j), borderwidth=2,relief='ridge', anchor="w") 
#        e.grid(row=i, column=j) 
        #e.insert(END, student[j])
#    i=i+1

#The right area (policy and list of policies)
###Rframe = tkinter.Frame(subRoot, width=220, height=300+deltaHeight, bg='white')
#Rframe.grid(row=0, column=3, padx=5, pady=5)

# The output goes here, at the bottom
###Bframe = tkinter.Frame(root, width=540, height=200, bg='purple')
#Bframe.grid(row=1, column=0, padx=5, pady=5)


#4, Runs the window's main loop.
root.mainloop()