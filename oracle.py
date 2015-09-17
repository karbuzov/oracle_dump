from globals import *
import sys, os
import threading
import Queue

diff_files = []

if len(sys.argv) <= 1:
    print "Usage: python oracle.py pilotX"
    exit()
    
DATABASE = sys.argv[1].upper()

EXPORT_TYPE = cmd_args.type.upper()

os.environ["ORACLE_HOME"] =  config['main']['oracle_home']
os.environ["PATH"] = config['main']['path'] + ";" + os.environ["PATH"]

global_path = config['main']['path_to_export_folder']

connections = {
  "PILOT":        {"USER": "CASINO_PILOT1", "PASSWORD": "CASINO_PILOT1", "TNS": "GRID_LGDBT"}
, "PILOT1":       {"USER": "CASINO_PILOT1", "PASSWORD": "CASINO_PILOT1", "TNS": "GRID_LGDBT"}
, "PILOT2":       {"USER": "CASINO_PILOT2", "PASSWORD": "CASINO_PILOT2", "TNS": "GRID_LGDBT"}
, "PILOT3":       {"USER": "CASINO_PILOT3", "PASSWORD": "CASINO_PILOT3", "TNS": "GRID_LGDBT"}
, "PILOT4":       {"USER": "CASINO_PILOT4", "PASSWORD": "CASINO_PILOT4", "TNS": "GRID_LGDBT"}
, "PILOT5":       {"USER": "CASINO_PILOT5", "PASSWORD": "CASINO_PILOT5", "TNS": "GRID_LGDBT"}
, "PILOT6":       {"USER": "CASINO_PILOT6", "PASSWORD": "CASINO_PILOT6", "TNS": "GRID_LGDBT"}
, "PILOT7":       {"USER": "CASINO_PILOT7", "PASSWORD": "CASINO_PILOT7", "TNS": "GRID_LGDBT"}
, "PILOT8":       {"USER": "CASINO_PILOT8", "PASSWORD": "CASINO_PILOT8", "TNS": "GRID_LGDBT"}
, "PILOT9":       {"USER": "CASINO_PILOT9", "PASSWORD": "casino_pilot9", "TNS": "GRID_CLDB2TS"}
, "PILOT10":      {"USER": "casino_pilot10", "PASSWORD": "casino_pilot10", "TNS": "GRID_CLDB2TS"}
, "PILOT11":      {"USER": "casino_pilot11", "PASSWORD": "casino_pilot11", "TNS": "GRID_CLDB2TS"}
, "PILOT12":      {"USER": "casino_pilot12", "PASSWORD": "casino_pilot12", "TNS": "GRID_CLDB2TS"}

, "PROCESSING":   {"USER": "CASINO_PROC", "PASSWORD": "CASINO_PROC", "TNS": "GRID_LGDBT", "DIR": "processing"}
, "BINGO":        {"USER": "BINGO", "PASSWORD": "BINGO", "TNS": "GRID_LGDBT"}
, "STUDIO":       {"USER": "CASINO_UNITY", "PASSWORD": "CASINO_UNITY", "TNS": "GRID_LGDBT"}
, "UNITY":        {"USER": "UNITY_WORK", "PASSWORD": "UNITY_WORK", "TNS": "GRID_LGDBT"}
, "MOBILE":       {"USER": "FLASHDEMO", "PASSWORD": "flashdemo", "TNS": "GRID_LGDBT"}
, "PRE-DEMO":     {"USER": "PREDEMO", "PASSWORD": "PREDEMO", "TNS": "GRID_LGDBT"}
, "INTEGRATION2": {"USER": "INTEGRATION", "PASSWORD": "INTEGRATION", "TNS": "GRID_LGDBT", "DIR": "INTEGRATION2"}
, "FLASHDEMO":    {"USER": "FLASHDEMO", "PASSWORD": "flashdemo", "TNS": "GRID_LGDBT"}
, "PREDEMO":      {"USER": "PREDEMO", "PASSWORD": "PREDEMO", "TNS": "GRID_LGDBT"}

, "PATCH":        {"USER": "TOPGAME5", "PASSWORD": "topgame5", "TNS": "GRID_DEMO", "DIR": "patch"}
, "INTEGRATION":  {"USER": "INTEGRATION", "PASSWORD": "integration", "TNS": "GRID_DEMO"}
, "DEMO":         {"USER": "DEMO", "PASSWORD": "demo", "TNS": "GRID_DEMO"}
, "TEST":         {"USER": "TEST_CASINO", "PASSWORD": "test_casino", "TNS": "GRID_DEMO", "DIR": "test"}
, "GAMESDEMO":    {"USER": "gamesdemo ", "PASSWORD": "gamesdemo", "TNS": "GRID_DEMO", "DIR": "gamesdemo"}

, "FREEBINGO":   {"USER": "BINGODK", "PASSWORD": "bingodk", "TNS": "GRID_GLGDBFR ", "DIR": "freebingo"}
, "BINGO-DEMO":   {"USER": "BINGODK", "PASSWORD": "bingodk", "TNS": "GRID_DEMO", "DIR": "BINGO-DEMO"}
}

def mystr(text):
    if text:
        return text
    else:
        return ""

def getfoldername(server_name):
    for i in connections.keys():
        if server_name.upper() == i:
            obj = connections[i]
            
            if server_name.find("PILOT") >= 0:
                return '' + obj['USER']
            
            if 'DIR' in obj and len(obj['DIR']) > 0:
                return '' + obj['DIR']
                
            return obj['USER']
    print "= = =   inaccessible servername:   = = =", server_name
    raise "inaccessible servername"


def getserver(server_name):
    for i in connections.keys():
        if server_name.upper() == i:
            obj = connections[i]
            return obj
    print " ======> inaccessible servername:", server_name
    raise "inaccessible servername"

import cx_Oracle
from xml.dom import minidom


class WorkerXMLObjectProcessor(threading.Thread):
    def __init__(self, object_type, params):
        super(WorkerXMLObjectProcessor, self).__init__()
        self.object_type = object_type
        self.params = params

    def run(self):
        try:
        self.process()
            
        except:
            print "!!!!!!!!!!!!!!!!!!!!!exception for type '%s' found!!!!!!!!!!!!!!!!!!!!!!!!!"%self.object_type, sys.exc_info()

    def process(self):
        
        sql = " select object_type, object_name, xml_lob \n"
        sql = " from ( \n"
        sql = "   select object_type, object_name, xml_lob from ( \n"
        sql +="     select DBMS_METADATA.GET_XML(object_type, object_name, USER) xml_lob, object_type, object_name \n"
        sql +="     from user_objects \n"
        sql +="     where 1 = 1 \n"
        sql +="    and object_name not like 'Z_%' \n"
        sql +="    and REGEXP_INSTR(object_name, '.*\d{1,5}$') = 0 \n"
#        sql += "    and object_name like 'MEMBER_%'       "
        sql +="     and object_type  = '" + self.object_type + "' \n"
        sql +="   ) \n"
        sql +=" order by object_type, object_name \n"

        db = oracle()
        db.connect(self.params["USER"], self.params["PASSWORD"], self.params["TNS"])
        c = db.connection.cursor()
        res = c.execute(sql)

        print object_type + "..."
        row = c.fetchone()
        obj = dbobjscript()
        obj.schema_name = 'casino'
        
        while row:
            obj_name = row[1]
            obj_type = row[0]
            
            xml_text = row[2].read();
            
            print obj_type, obj_name
            
            obj.process_xml(obj_name, obj_type, xml_text)

            row = c.fetchone()
        db.close()

class oracle:
    db_connection = False

    def __init__(self):
        None

    def connect(self, user, password, tns_name):
        self.user = user
        self.password = password
        self.tns_name = tns_name
        self.connection = cx_Oracle.connect(self.user + "/" + self.password + "@" + self.tns_name)

    def execsql(self, sql):
        c = self.connection.cursor()
        res = c.execute(sql)
        return  res

    def close(self):
        self.connection.close();

    def execsql_2(self, sql):
        c = self.connection.cursor()
        res = c.execute(sql)
        return res.fetchall()


class dbobjscript:
    schema_name = ''
    
    def __init__(self):
        None
        
    def _getxmlvalue (self, xmlnode, tagname):
        xmlval = xmlnode.getElementsByTagName(tagname)
        if len(xmlval) >0:
            return xmlval[0].firstChild.data
            
        return ""
        
    def getxmlvalue (self, xmlNodeToFind, tagname):
        xmlnodes = xmlNodeToFind.getElementsByTagName(tagname)
        for xmlnode in xmlnodes:
            
            if xmlNodeToFind == xmlnode.parentNode:
                return xmlnode.firstChild.data 
            
        return ""

    def getxmlvalue2 (self, xmlnode, tagname):
        text = ""
        print "getxmlvalue::", tagname
        for node in xmlnode.childNodes:
            print "====", node.localName, "<<>>", node.nodeType, "::", node.nodeValue
            if node.nodeType == node.TEXT_NODE:
                text = node.data
                print "==================>", text, "==", node.nodeValue
                #return node.data
            if node.nodeType == node.ELEMENT_NODE and node.localName == tagname:
                print "==================>", text
                #return text
        raise 1
        return ""
    
    def getxmlChildNode (self, xmlnode, tagname):
        xmlval = xmlnode.getElementsByTagName(tagname)
        if len(xmlval) >0:
            return xmlval[0]
            
        return False
        
    def myjoin(self, separator, array):
        i = 0
        c = 1
        result = ""
        l = len(array)
        for item in array:
            if l > 1:
                if len(result) + len(item) + 0 > 70 * c + 1:
                    result += "\n "
                    c += 1
                result += item;
                if i + 1 < l:
                    result += separator
            i+= 1
        return result
        
    def gettype_(self, num, additional_info, scale):
        TYPES = {
         "1":"VARCHAR2"
        ,"96":"CHAR"
        ,"2":"NUMBER"
        ,"12":"DATE"
        ,"112":"CLOB"
        ,"128":"CLOB"
        ,"113":"BLOB"
        ,"180":"TIMESTAMP"
        ,"8":"LONG"
        ,"2":"NUMBER"
        ,"231":"TIMESTAMP(6) WITH LOCAL TIME ZONE"
        ,"181":"TIMESTAMP(6) WITH TIME ZONE"
        ,"-113":"--------------"
        }
        for i in TYPES.keys():
            if i == num:
#                if num == "2" and scale == "0":
#                    return "INTEGER"
                    
                return TYPES[i]
        print "= = =   undefined column type: " + num + " COLUMN: " + additional_info + "  = = =";
        raise "undefined column type: " + num;

        
    def getlenthtype(self, xmlItem, column_type):

        charsetform         = self.getxmlvalue(xmlItem, "CHARSETFORM")
        typeid              = self.getxmlvalue(xmlItem, "PROPERTY")
        precision           = self.getxmlvalue(xmlItem, "PRECISION_NUM")
        length              = self.getxmlvalue(xmlItem, "LENGTH")
        col_len_spare       = self.getxmlvalue(xmlItem, "SPARE3")
        col_len_def         = self.getxmlvalue(xmlItem, "SCALE")
        column_name         = self.getxmlvalue(xmlItem, "NAME")
        
        #print "============================", charsetform, typeid, precision, "<><><>=", length, col_len_spare, col_len_def, column_name
        
        if int(col_len_spare) > 0:
            length = col_len_spare
        
        if precision:
            if col_len_def and col_len_def != "0":
                return "(" + precision + "," + col_len_def + ")"
            return "(" + precision + ")"

        if charsetform == "0":
            if typeid == "0" or typeid == "128":
                return ""
                
            if column_type == "NUMBER" and length == "22":
                return ""
                
            return "(" + length + ")"
        
            if typeid == "0":
                return "(" + length + " BYTE)"
            
        if charsetform == "1":    
            if typeid == "8388608":
                return "(" + str(int(length) / 1) + " CHAR)"
        
            if typeid == "0" or typeid == "1073741824":
                if length == "0":
                    return ""
                return "(" + length + " BYTE)"

            if typeid == "128":
                return ""

            if typeid == "180":
                return "(" + length + ")"

            if typeid == "-65832":
                return "(" + length + ")"
                
        print "undefined type >>> ", column_name, "charsetform:", charsetform, "typeid:", typeid, "length:", length, "precision:", precision
        raise "undefined ~!!!!!!!!!!##############: "
    
    
    def save_constr_to_f(self, constraints, table_name):
        from operator import itemgetter
        
        new_constraints = sorted(constraints, key=itemgetter('sort_order', 'name'))
        
        file_data = ''
        
        cur_type = 0
        first_row = True
        
        fname1 = global_path2 + self.schema_name + "/Constraints/" + table_name + ".sql"
        
        for constraint in new_constraints:
            if cur_type != constraint["sort_order"]:

                if first_row:
                    first_row = False
                else:
                    file_data += "\n\n"

                if constraint['sort_order'] == 1:
                    file_data += "-- PRIMARY\n"
                elif constraint['sort_order'] == 2:
                    file_data += "-- UNIQUE\n"
                elif constraint['sort_order'] == 3:
                    file_data += "-- CHECK\n"
                elif constraint['sort_order'] == 4:
                    file_data += "-- FOREIGN\n"
                
                cur_type = constraint["sort_order"]
                
            file_data += "\n" + constraint["text"] + "\n"

#        print file_data, "--------------------", fname1
        if len(file_data) > 0:
            save2file(fname1, file_data)

    def check_culumn_is_system(self, xmlNode):
        col_name           = self.getxmlvalue(xmlNode, "NAME")
        col_num            = self.getxmlvalue(xmlNode, "INTCOL_NUM")
        
        if col_name.find('SYS_') == 0:
            if col_name.find(col_num + "$") == len(col_name) - len(col_num + "$"):
                return True;
            
        return False;
    

    def parse_columns(self, items2, constraints_obj):
        
        text = ''

        max_column_len = 0
        for item in items2:
            if self.check_culumn_is_system(item):
                continue
            col_name            = self.getxmlvalue(item, "NAME")
            
            if len(col_name) > max_column_len:
                max_column_len = len(col_name)

        max_column_len += 2
        rows_ = []
        
        for item in items2:
            if self.check_culumn_is_system(item):
                continue

            
            col_name            = self.getxmlvalue(item, "NAME")
            col_type            = self.getxmlvalue(item, "TYPE_NUM")
            scale               = self.getxmlvalue(item, "SCALE")
            NOT_NULL            = self.getxmlvalue(item, "NOT_NULL")
            constraint_node     = self.getxmlChildNode(item, "CON")
            default_value       = self.getxmlvalue(item, "DEFAULT_VAL")

            column_text = "  " + col_name.ljust(max_column_len)
            
            type123 = self.gettype_(col_type, "column: " + col_name, scale);
            
            type123 = type123  + self.getlenthtype(item, type123)
            defaultstr = ""
            constraintstr = ""
            notnullstr = ""
            
            if default_value:
                defaultstr = " DEFAULT " + default_value.replace("&apos;", "'").replace("\n", "")

            if constraint_node:
                constraint_name = self.getxmlvalue(constraint_node, "NAME")
                
                if constraint_name.find("SYS_") == -1:
                    constraintstr = " CONSTRAINT " + constraint_name
            
            if NOT_NULL != "" and NOT_NULL != "0":
                constraint_exists = False
#                for constraint in constraints_obj:
#                    if constraint['text'].find("PRIMARY KEY") > 0 and constraint['text'].find(col_name) > 0:
#                        constraint_exists = True
#                if constraint_exists:
#                    text = text + type123
#                else:
                notnullstr = " NOT NULL"
            
            row_ = {"type123":          type123, 
                    "defaultstr":       defaultstr, 
                    "constraintstr":    constraintstr, 
                    "notnullstr":       notnullstr, 
                    "column_text":      column_text, 
                    "rowtext":          ""}
            rows_.append(row_)
            
        isfirstrow = True
        text = ''
        
        for columnd in rows_:
            columnd["rowtext"] = columnd["rowtext"] + columnd["column_text"]
        
        maxlen = 0
        for columnd in rows_:
            columnd["rowtext"] = columnd["rowtext"] + columnd["type123"]
            l = len(columnd["rowtext"])
            if l > maxlen:
                maxlen = l


        maxlen2 = 0
        for columnd in rows_:
            columnd["rowtext"] = columnd["rowtext"].ljust(maxlen + 5)

            if columnd["defaultstr"]:
                columnd["rowtext"] = columnd["rowtext"] + columnd["defaultstr"] + columnd["constraintstr"]
            else:
                columnd["rowtext"] = columnd["rowtext"] + columnd["constraintstr"] + columnd["notnullstr"]

            l = len(columnd["rowtext"])
            if l > maxlen2:
                maxlen2 = l


        maxlen = 0
        for columnd in rows_:
            columnd["rowtext"] = columnd["rowtext"].ljust(maxlen + 8)

            if columnd["defaultstr"] and columnd["notnullstr"]:
                columnd["rowtext"] = columnd["rowtext"].ljust(maxlen2 + 14) + columnd["notnullstr"]

        rl = len(rows_)
        i = 0
        for columnd in rows_:
            i += 1
            if i <> rl:
                text = text + columnd["rowtext"].rstrip() + ",\n"
            else:
                text = text + columnd["rowtext"].rstrip()
        return text
        
        
        for columnd in rows_:
            if not isfirstrow:
                text = text + ",\n";
            isfirstrow = False
            
            if len(columnd["defaultstr"]) > 0:
                addstr = columnd["column_text"] + columnd["type123"].ljust(type_max_len + 8) + columnd["defaultstr"]
            else:
                addstr = columnd["column_text"] + columnd["type123"]
            
            addstr = addstr + columnd["constraintstr"]
            if len(columnd["notnullstr"]) > 0:
                if len(columnd["defaultstr"]) > 0:
                    addstr = addstr + "".ljust(30-len(columnd["defaultstr"])) + columnd["notnullstr"]
                else:
                    addstr = addstr.ljust(24) + columnd["notnullstr"]
            text = text + addstr
#        print " * * ** * *  * * *", text
#        raise 111111111111111111111111
        return text


    def parse_constraints(self, table_name, items2):
        CONSTRAINTS = []
        text = ''
        for item in items2:
            
            NAME      = self.getxmlvalue(item, "NAME")
            PROPERTY  = self.getxmlvalue(item, "PROPERTY")
            NUMCOLS   = self.getxmlvalue(item, "NUMCOLS")
            CONTYPE   = self.getxmlvalue(item, "CONTYPE")
            ENABLED   = self.getxmlvalue(item, "ENABLED")
            CONDITION = self.getxmlvalue(item, "CONDITION")
            ENABLED   = self.getxmlvalue(item, "ENABLED")
            ENABLED   = self.getxmlvalue(item, "ENABLED")
            ENABLED   = self.getxmlvalue(item, "ENABLED")
            VALIDATE  = self.getxmlvalue(item, "FLAGS")
            
            text = ''
            
            text = text + 'ALTER TABLE ' + table_name + ' ADD (\n'
            text = text + '  CONSTRAINT ' + NAME + ' \n'
            
            processed = False
            columns_a = []
            sort_order = 0;
            
            if CONTYPE == "1":
                text = text + '  CHECK ' + '(' + CONDITION + ')'
                processed = True
                sort_order = 3;
                
            if CONTYPE == "2" or CONTYPE == "3":
                columns = ''
                
                COL_LIST_ITEMs     = item.getElementsByTagName("COL_LIST_ITEM")
                for COL_LIST_ITEM in COL_LIST_ITEMs:
                    COLs = COL_LIST_ITEM.getElementsByTagName("COL")
                    for COL in COLs:
                        columnname = self.getxmlvalue(COL, "NAME")
                        
                        if columnname not in columns_a:
                            columns_a.append(columnname)
                        
                if CONTYPE == "3":
                    text = text + '  UNIQUE (' + ', '.join(columns_a) + ')\n  USING INDEX ' + NAME
                    sort_order = 2;
                if CONTYPE == "2":
                    text = text + '  PRIMARY KEY \n  (' + ', '.join(columns_a) + ')\n'
                    sort_order = 1;
                processed = True
            
            columns_a = []
            
            if CONTYPE == "4":
                SCHEMA_OBJ      = item.getElementsByTagName("SCHEMA_OBJ")[0]
                target_table    = self.getxmlvalue(SCHEMA_OBJ, "NAME")
                
                SRC_COL_LIST_ITEMs    = item.getElementsByTagName("SRC_COL_LIST_ITEM")

                for SRC_COL_LIST_ITEM in SRC_COL_LIST_ITEMs:
                    COLs = SRC_COL_LIST_ITEM.getElementsByTagName("COL")
                    for COL in COLs:
                        columnname = self.getxmlvalue(COL, "NAME")
                        if columnname not in columns_a:
                            columns_a.append(columnname)
                
                text = text + '  FOREIGN KEY (' + ', '.join(columns_a) + ') \n'
                sort_order = 4;

                columns_a = []
                SCHEMA_OBJ      = item.getElementsByTagName("TGT_COL_LIST")[0]
                TGT_COL_LIST_ITEMs    = item.getElementsByTagName("TGT_COL_LIST_ITEM")
                
                for TGT_COL_LIST_ITEM in TGT_COL_LIST_ITEMs:
                    COLs = TGT_COL_LIST_ITEM.getElementsByTagName("COL")
                    for COL in COLs:
                        columnname = self.getxmlvalue(COL, "NAME")
                        if columnname not in columns_a:
                            columns_a.append(columnname)
                
                text = text + '  REFERENCES ' + target_table + ' (' + ', '.join(columns_a) + ')'
               
                processed = True
            
            if processed == False :
                print "undefined CONSTRAINT TYPE >>> ", CONTYPE
                raise "undefined CONSTRAINT TYPE##############: "
    
            if ENABLED == "0":
                text = text + '\n  DISABLE'
            
            text = text +  ");"
            
            CONSTRAINT = {"name": NAME, "sort_order": sort_order, "type": CONTYPE, "text": text}
            CONSTRAINTS.append(CONSTRAINT)
        return CONSTRAINTS

    
    def export_table(self, xml_data):
        
        doc = minidom.parseString(xml_data)
        
        items1 = self.getxmlChildNode(doc, "SCHEMA_OBJ")
        
        obj_type = self.getxmlvalue(items1, "TYPE_NAME")
        obj_name = self.getxmlvalue(items1, "NAME")
        flags = self.getxmlvalue(items1, "FLAGS")
        
        text = "CREATE " + obj_type + " " + obj_name + "\n(\n";
        if flags == "2":
            text = "CREATE GLOBAL TEMPORARY " + obj_type + " " + obj_name + "\n(\n";
        
        docColumns = doc.getElementsByTagName("COL_LIST")
        docColumn = docColumns[0]
        
        text_columns = ''
        COL_LIST_ITEMs = docColumn.getElementsByTagName("COL_LIST_ITEM")
        
###     # NOT FK_ CONSTRAINTS
        docColumns = doc.getElementsByTagName("CON1_LIST")
        docColumn = docColumns[0]
 
        CON1_LIST_ITEM = docColumn.getElementsByTagName("CON1_LIST_ITEM")
        constraints1 = self.parse_constraints(obj_name, CON1_LIST_ITEM)
        
        docColumns = doc.getElementsByTagName("CON2_LIST")
        docColumn = docColumns[0]

        CON2_LIST_ITEM = docColumn.getElementsByTagName("CON2_LIST_ITEM")
        constraints = self.parse_constraints(obj_name, CON2_LIST_ITEM)
        constraints += constraints1
        
        self.save_constr_to_f(constraints, obj_name)
###     # end of FK_ CONSTRAINTS

        text_columns = self.parse_columns(COL_LIST_ITEMs, constraints1)
        if flags == "2":
            text = text + text_columns + "\n)\nON COMMIT PRESERVE ROWS;\n\n\n"
        else:
            text = text + text_columns + "\n);\n\n\n"
        return text
    

    def export_view(self, xml_data):
        doc = minidom.parseString(xml_data)
        
        VIEW_T          = self.getxmlChildNode(doc, "VIEW_T")
        PROPERTY        = self.getxmlvalue(VIEW_T, "PROPERTY")
        SCHEMA_OBJ      = self.getxmlChildNode(doc, "SCHEMA_OBJ")
        VIEW_NAME       = self.getxmlvalue(SCHEMA_OBJ, "NAME")
        OBJ_TEXT        = self.getxmlvalue(VIEW_T, "TEXT")
        
        text = 'CREATE OR REPLACE FORCE VIEW ' + VIEW_NAME + ''
        columns_a = []
        
        COL_LIST        = self.getxmlChildNode(doc, "COL_LIST")
        COL_LIST_ITEMs  = COL_LIST.getElementsByTagName("COL_LIST_ITEM")
        for COL_LIST_ITEM in COL_LIST_ITEMs:
            columnname = self.getxmlvalue(COL_LIST_ITEM, "NAME")
            
            if columnname not in columns_a:
                columns_a.append(columnname)
        
        for i in range(0, 2):
            if OBJ_TEXT[-1:] == "\n" or OBJ_TEXT[-1:] == " ":
                OBJ_TEXT = OBJ_TEXT[:(len(OBJ_TEXT)-1)]
            
        return text + '\n(' + self.myjoin(', ', columns_a) + ')\nAS \n' + OBJ_TEXT + ';\n\n\n'
   

    def export_index(self, xml_data):
        doc = minidom.parseString(xml_data)
        
        INDEX_T         = self.getxmlChildNode(doc, "INDEX_T")
        PROPERTY        = self.getxmlvalue(INDEX_T, "PROPERTY")
        SCHEMA_OBJ      = self.getxmlChildNode(doc, "SCHEMA_OBJ")
        NUMKEYCOLS      = self.getxmlChildNode(doc, "NUMKEYCOLS")
        
        INDEX_NAME      = self.getxmlvalue(SCHEMA_OBJ, "NAME")
        
        BASE_OBJ        = self.getxmlChildNode(doc, "BASE_OBJ")
        TABLE_NAME      = self.getxmlvalue(BASE_OBJ, "NAME")
        
        text = 'CREATE INDEX ' + INDEX_NAME + ' '

        #if PROPERTY and PROPERTY == "1" or PROPERTY == "3":
        if PROPERTY and int(PROPERTY)%2 == 1:
            text = 'CREATE UNIQUE INDEX ' + INDEX_NAME + ' '
        text = text + 'ON ' + TABLE_NAME + '\n'
        columns_a = []
        
        COL_LIST        = self.getxmlChildNode(doc, "COL_LIST")
        
        COL_LIST_ITEMs  = COL_LIST.getElementsByTagName("COL_LIST_ITEM")
        for COL_LIST_ITEM in COL_LIST_ITEMs:
            COLs = COL_LIST_ITEM.getElementsByTagName("COL")
            for COL in COLs:
                columnname = self.getxmlvalue(COL, "NAME")
                DEFAULT_VAL = self.getxmlvalue(COL, "DEFAULT_VAL")
                
                if DEFAULT_VAL != "" and columnname[:4] == "SYS_" and columnname[-1:] == "$":
                    columnname = DEFAULT_VAL

                if columnname not in columns_a:
                    columns_a.append(columnname)
        
        compress = ""
        if NUMKEYCOLS:
            compress = "\nCOMPRESS 1"
                    
        return text + '(' + ', '.join(columns_a) + ')' + compress + ';\n\n\n'


    def save_program_blocks(self, db):
        types = db.execsql_2("select user from dual")
        user_name = types[0][0]
        
        disabled_triggers_ = db.execsql_2("select trigger_name from user_triggers where status <> 'ENABLED'")
        disabled_triggers = []
        for tr in disabled_triggers_:
            disabled_triggers.append(tr[0])
        
        types = db.execsql_2("select distinct type, name from user_source where name not like 'Z_%' and name not like '%_TEMP' and name not like '%_TEMP_PKG' and type <> 'JAVA SOURCE' and name not like 'BIN$%==$0' and type <> 'PROCEDURE' and type <> 'FUNCTION' order by 1, 2")

        for row in types:
            type = row[0]
            name = row[1]
            
            fname = ''
            
            file_extension = '.sql'
            if type == 'TRIGGER':
                fname = 'Triggers'
                file_extension = '.trg'
            if type == 'FUNCTION':
                fname = 'Functions'
                file_extension = '.fnc'
            if type == 'PROCEDURE':
                fname = 'Procedures'
                file_extension = '.prc'
            if type == 'PACKAGE BODY':
                fname = 'PackageBodies'
                file_extension = '.pkb'
            if type == 'PACKAGE':
                fname = 'Packages'
                file_extension = '.pks'
            if type == 'JAVA SOURCE':
                fname = 'JavaSources'
                file_extension = '.jvs'
            if type == 'TYPE':
                fname = 'Types'
                file_extension = '.tps'
            
            if len(fname) < 3:
                print "undefined source type: ", type
                raise 1
            
            fname = global_path2 + self.schema_name + '/' + fname
            if not os.path.exists(fname):
                os.makedirs(fname)
                
            fname += '/' + name + file_extension

            sql = "select text from user_source where type = '" + type + "' and name = '" + name + "'  order by line"
            text_rows = db.execsql_2(sql)
            rowscount = len(text_rows)
            currow = 1;
            cur_str = ''
            file_content = ''
            for text_row in text_rows:
                if text_row[0]:
                    cur_str = text_row[0]
                    if currow == 1 or (type == 'TRIGGER' and currow < 6):
                    
                        if cur_str.upper().find(user_name.upper()) > 0:
#                            if cur_str.find('"' + user_name + '"."') > 0:
#                                cur_str = cur_str.replace('"' + user_name'"."', '')
#                                cur_str = cur_str.replace('"', '')
                            cur_str = cur_str.replace('"' + user_name + '".', '')
                            cur_str = cur_str.replace('"' + user_name.upper() + '".', '')
                            cur_str = cur_str.replace(user_name + '.', '')
                            cur_str = cur_str.replace(user_name.upper() + '.', '')
                            cur_str = cur_str.replace('"', '')
                    if currow == 1:
                        cur_str = 'CREATE OR REPLACE ' + cur_str
                        
                    if not (currow == rowscount and cur_str == " "):
                        file_content += cur_str
                currow += 1
            
            file_content = file_content;
            if type == 'TRIGGER':
                if name in disabled_triggers:
                    file_content = file_content.replace("FOR EACH ROW\n", "FOR EACH ROW\nDISABLE\n")
                
                file_content = file_content.replace(name.lower(), name)
                file_content = file_content.strip();
                save2file(fname, file_content + "\n/\n\n\n")
            else:
                if file_content[-1:] == "\n":
                    save2file(fname, file_content + "\n/\n\n")
                else:
                    save2file(fname, file_content + "\n\n/\n\n")
            
            print cmd_args.dbname, type, name
            

    def export_sequence(self, xml_data):
        doc = minidom.parseString(xml_data)
        
        TopElement      = self.getxmlChildNode(doc, "SEQUENCE_T")
        SCHEMA_OBJ      = self.getxmlChildNode(doc, "SCHEMA_OBJ")
        
        OBJECT_NAME     = self.getxmlvalue(SCHEMA_OBJ, "NAME")
        CYCLE           = self.getxmlvalue(TopElement, "CYCLE")
        CACHE           = self.getxmlvalue(TopElement, "CACHE")
        MINVALUE        = self.getxmlvalue(TopElement, "MINVALUE")
        MAXVALUE        = self.getxmlvalue(TopElement, "MAXVALUE")
        SEQ_ORDER       = self.getxmlvalue(TopElement, "SEQ_ORDER")
        INCRE           = self.getxmlvalue(TopElement, "INCRE")
        
        text = 'CREATE SEQUENCE ' + OBJECT_NAME + '\n'
        text += '  START WITH ' +  MINVALUE + '\n'
        
        if INCRE != "1":
            text += '  INCREMENT BY ' +  INCRE + '\n'
        
        text += '  MAXVALUE ' +  MAXVALUE + '\n'
        text += '  MINVALUE ' +  MINVALUE + '\n'
        
        if int(CYCLE) == 0:
            text += "  NOCYCLE\n"
        else:
            text += "  CYCLE\n"

        if int(CACHE) == 0:
            text += "  NOCACHE\n"
        else:
            text += "  CACHE " + CACHE + "\n" 
        
        if int(SEQ_ORDER) == 0:
            text += "  NOORDER"
        else:
            text += "  ORDER"
        
        return text + ";\n\n\n"
        

    def export_synonym(self, xml_data):
        doc = minidom.parseString(xml_data)
        
        TopElement      = self.getxmlChildNode(doc, "SYNONYM_T")
        SCHEMA_OBJ      = self.getxmlChildNode(doc, "SCHEMA_OBJ")
        
        OBJECT_NAME     = self.getxmlvalue(SCHEMA_OBJ, "NAME")
        OWNER_NAME      = self.getxmlvalue(SCHEMA_OBJ, "OWNER_NAME")

        SYN_LONG_NAME   = self.getxmlvalue(TopElement, "SYN_LONG_NAME")
        SYN_NAME        = self.getxmlvalue(TopElement, "NAME")
        syn_OWNER_NAME  = self.getxmlvalue(TopElement, "OWNER_NAME")
        
        text = 'CREATE SYNONYM ' + OBJECT_NAME + ' FOR ' 
        
        if syn_OWNER_NAME != OWNER_NAME:
            text += syn_OWNER_NAME + '.'
        
        text += SYN_NAME

        return text + ";\n\n\n"
        

    def process_xml(self, objname, objtype, xml_data):
        text = ''
        if objtype == 'TABLE':
            text = self.export_table(xml_data);
            save2file(global_path2 + self.schema_name + "/Tables/" + objname + ".sql", text)

        if objtype == 'SEQUENCE':
            text = self.export_sequence(xml_data);
            save2file(global_path2 + self.schema_name + "/Sequences/" + objname + ".sql", text)

        if objtype == 'SYNONYM':
            text = self.export_synonym(xml_data);
            save2file(global_path2 + self.schema_name + "/Synonym/" + objname + ".sql", text)

        if objtype == 'VIEW':
            text = self.export_view(xml_data);
            save2file(global_path2 + self.schema_name + "/Views/" + objname + ".vw", text)

        if objtype == 'INDEX':
            text = self.export_index(xml_data);
            save2file(global_path2 + self.schema_name + "/Indexes/" + objname + ".sql", text)
        
#        if objtype == 'INDEX':
#            text = self.export_index(xml_data);
#            save2file(global_path2 + self.schema_name + "/Indexes/" + objname + ".sql", text)
        
        if len(text) == 0:
            print "error when processing object. No type handler found: ", objtype, objname
            raise 1

def __prep(content):

    endoffirstrow = content.find(",\n")
    if endoffirstrow > 0:
        part1 = content[0:endoffirstrow]
        part2 = content[endoffirstrow:len(content)]
        
        content = part1.replace("NOT NULL", "") + part2
        

    
    content = content.replace("        ", "")
    content = content.replace("       ", "")
    content = content.replace("      ", "")
    content = content.replace("     ", "")
    content = content.replace("    ", "")
    content = content.replace("   ", "")
    content = content.replace("  ", "")
    content = content.replace("  ", "")
    content = content.replace("  ", "")
    content = content.replace("  ", "")
    content = content.replace("  ", "")
    content = content.replace("  ", "")
    content = content.replace("	", "")
    content = content.replace("	", "")
    content = content.replace("	", "")
    content = content.replace("	", "")
    content = content.replace(" ", "")
    content = content.replace(" ", "")
    content = content.replace(" ", "")
    content = content.replace(" ", "")
    content = content.replace("\n", "")
    content = content.replace("\n", "")
    
    try:
        if content[0] == '\xEF':
            content = content.replace('\xEF\xBB\xBF', "")
    except Exception:
        pass
        
    return content
            
def is_file_realy_changed(content1, content2):
    content1 = __prep(content1)
    content2 = __prep(content2)
    
#    print " = = = = = = == ", len(content1), "==" , len(content2)
    
    if len(content1) == len(content2):
        return False
    else:
        #print "******************\n", content1, "\n=====================\n", content2, "\n*******", len(content1), "==", len(content2), "\n\n\n"
        return True
            
def save2file(fname, data):
    content = ''
    if os.path.isfile(fname):
        with open(fname, "r") as text_file:
            content = text_file.read()

    if True == is_file_realy_changed(data, content):
        #print fname, "******************\n", data, "=====================\n", content
        diff_files.append(fname)
        print " Writing file ", fname
        #print "******************\n"
        with open(fname, "w") as text_file:
            text_file.write(data)

# =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =         

global_path2 = global_path + getfoldername(str(cmd_args.dbname)) + '/'
object_types = ['INDEX', 'TABLE', 'VIEW', 'SEQUENCE', 'SYNONYM']
#object_types = ['VIEW', 'SEQUENCE', 'TABLE', 'SYNONYM']
#object_types = ['TABLE']

if EXPORT_TYPE == "FAST":
    object_types = []

if not os.path.exists(global_path2):
    print "path '", global_path2, "' does not exists"
    raise 'critical error'

#print "----------------------------------------------------------------------------"
#print  global_path2
#print "----------------------------------------------------------------------------"
#print ")))))))))))))))))))))", global_path2
#raise '||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||'

#   select DBMS_METADATA.GET_XML('TABLE', 'CASINO', USER) from dual

try:
    workers = []
    for object_type in object_types:

        worker = WorkerXMLObjectProcessor(object_type, getserver(cmd_args.dbname))
        worker.start()
        workers.append(worker)
except Exception as e:
    print(e)

   
# export Oracle packages, triggers, functions and others...
import time
start_time = time.time()

try:
    db = oracle()
    params = getserver(cmd_args.dbname)
    db.connect(params["USER"], params["PASSWORD"], params["TNS"])

    obj = dbobjscript()
    obj.schema_name = 'casino'
    obj.db_connection = db
    obj.save_program_blocks(db)
    db.close()
except Exception as e:
    print(e)

for worker in workers:
    worker.join()
    
print cmd_args.dbname + " done. --- %s seconds ---" % (time.time() - start_time)
raw_input("Press Enter to continue...")


import subprocess
subprocess.call("python " + os.path.dirname(os.path.abspath(__file__)) + "\\releaser.py " + params["USER"] + " 16.0")

print cmd_args.dbname + " done. --- %s seconds ---" % (time.time() - start_time)
raw_input("Press Enter to continue...")
