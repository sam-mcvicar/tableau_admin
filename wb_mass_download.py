import psycopg2
import tableauserverclient as TSC
import os 
dir_path = os.path.dirname(os.path.realpath(__file__))
tableau_auth = TSC.TableauAuth('svctableauserver', 'R3p0rtOnThis')
server = TSC.Server('http://tableau-dev')

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(host="tab-ict-82",database="workgroup", user="readonly", password="Welcome1",port="8060" )
 
        # create a cursor
        cur = conn.cursor()
        
        # execute a statement
        print('PostgreSQL database version:')
        cur.execute("""SELECT 
                        wb.luid
                        FROM workbooks wb
                        LEFT OUTER JOIN projects pr
                        on wb.Project_ID = pr.ID
                        WHERE pr.name = 'Analytics'""")
 
        # display the PostgreSQL database server version
        low_view_wb = cur.fetchall()
        print("Low usage workbooks retrieved")
        low_workbooks = [i[0] for i in low_view_wb]

        c = 0
        #while c < len(low_workbooks):
        wb = low_workbooks[0]
        print(len(low_workbooks))
        with server.auth.sign_in(tableau_auth):        
            for c in range(len(low_workbooks)):
                workbook = server.workbooks.get_by_id(wb)
                print(workbook.name)
                server.workbooks.download(wb)
                #server.workbooks.delete(wb)
                c += 1
                wb = low_workbooks[c]

        print("Downloaded {0} workbooks to {1}".format(c,dir_path))

     # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    except Exception as gen_error:
        print ("Unexpected error: {0}".format(gen_error))
        raise
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


 
if __name__ == '__main__':
    connect()
