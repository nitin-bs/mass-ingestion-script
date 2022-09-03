import json
import os
import re
from typing import List

import pandas as pd
from antlr4 import *
from colorama import Fore, Style

from parser.TeradataLexer import TeradataLexer
from parser.TeradataListener import TeradataListener
from parser.TeradataParser import TeradataParser
import argparse
cwd=os.getcwd()
configuration_file= open(f"{cwd}/conf/configurations.json","r")
configuration_json=json.load(configuration_file)
class TPTExpressionExtractor(TeradataListener):

    def __init__(self, stream: CommonTokenStream):
        self.column_expr = {}
        self.tokens: CommonTokenStream = stream

    def getColumnExpr(self):
        return self.column_expr

    def exitInsertCallback(self, ctx: TeradataParser.InsertCallbackContext):
        insertIntoContext: TeradataParser.InsertIntoContext = ctx.insertInto()
        identifierList: TeradataParser.IdentifierListContext = insertIntoContext.identifierList()
        valuesExpression: TeradataParser.InlineTableContext = insertIntoContext.valuesExpression()
        if identifierList is not None and valuesExpression is not None:
            seq: TeradataParser.IdentifierSeqContext = identifierList.identifierSeq()
            column_identifiers: List[TeradataParser.IdentifierContext] = seq.identifier()

            for index, column_identifier in enumerate(column_identifiers):
                column_identifier: TeradataParser.IdentifierContext = column_identifier
                values_expression: TeradataParser.ExpressionContext = valuesExpression.expression(i=index)
                self.column_expr[column_identifier.getText()] = \
                    self.tokens.getText(values_expression.start, values_expression.stop)


def extract_tpt_columns(sql):
    try:
        print(f"Parsing SQL: {sql}")
        data = InputStream(sql)
        lexer = TeradataLexer(data)
        stream = CommonTokenStream(lexer)
        parser = TeradataParser(stream)
        tree = parser.statement()
        extractor = TPTExpressionExtractor(stream)
        walker = ParseTreeWalker()
        # visitor = TPTColumnExtractor()
        # visitor.visit(tree)

        walker.walk(extractor, tree)
        return extractor.getColumnExpr(),""
    except Exception as e:
        return {},str(e)


def find_between(s,start,end):
    try:
        result=(s.split(start))[1].split(end)[0]
        return result
    except IndexError as e:
        print(f"Could not find the statement that starts with {start} and ends with {end}")
        print(str(e))
        return ""

def print_success(message):
    print(Fore.GREEN + message)
    print(Style.RESET_ALL)

def print_warn(message):
    print(Fore.YELLOW + message)
    print(Style.RESET_ALL)
def print_error(message):
    print(Fore.RED + message)
    print(Style.RESET_ALL)

def remove_null_values_columns(insert_sql,filename):
    columns = find_between(insert_sql, f"INSERT INTO ", "VALUES").strip()
    columns = re.sub(".*?\(","",columns)
    columns=columns[1:-1].split(",")
    columns=list(map(lambda x:x.strip(),columns))
    values = find_between(insert_sql, "VALUES", ");").strip()
    values = values[1:]
    values=re.split(r',\s*(?![^()]*\))', values)
    values = list(map(lambda x: x.strip(),values))
    final_list=list(zip(columns, values))
    final_list = list(filter(lambda x: x[1] != 'NULL', final_list))
    final_sql = f"INSERT INTO {filename} ({','.join([x[0] for x in final_list])})  VALUES ({','.join([x[1] for x in final_list])});"
    return final_sql

def add_function_mappings(insert_sql):
    if insert_sql.lower().startswith("case when"):
        return insert_sql
    insert_sql=re.sub("(DECIMAL\(\d+,\d+\))","",insert_sql)
    insert_sql = re.sub("\(AS CHAR\(\d+\)\)", "", insert_sql)
    col_name = re.search(":.+?(?=(,|\s|\)))", insert_sql + ",")
    if not col_name:
        return f"lit({insert_sql})"
    else:
        col_name = col_name.group().strip().strip(":")
    if bool(re.search("(AS TIMESTAMP\(\d\) FORMAT| as timestamp\(\d\) format)",insert_sql)):
        if bool(re.search("((trim|TRIM)\s*\([:a-zA-Z0-9_]+\))",insert_sql)):
            insert_sql=re.sub("((trim|TRIM)\s*\([:a-zA-Z0-9_]+\))",f":{col_name}",insert_sql)
        if bool(re.search("((substr|SUBSTR)\s*\([:a-zA-Z0-9_]+,\d+,\d+\))",insert_sql)):
            insert_sql=re.sub("((substr|SUBSTR)\s*\([:a-zA-Z0-9_]+,\d+,\d+\))",f":{col_name}",insert_sql)
        insert_sql = re.sub("(AS TIMESTAMP\(\d\) FORMAT| as timestamp\(\d\) format)",",",insert_sql)
        insert_sql=re.sub("(CAST|cast|Cast)","to_timestamp",insert_sql)
        if insert_sql.startswith("("):
            insert_sql = insert_sql.replace("(", "to_timestamp(", 1)
    if bool(re.search("(AS TIMESTAMP\(\d\)\)|as timestamp\(\d\)\))",insert_sql)):
        if bool(re.search("((trim|TRIM)\s*\([:a-zA-Z0-9_]+\))",insert_sql)):
            insert_sql=re.sub("((trim|TRIM)\s*\([:a-zA-Z0-9_]+\))",f":{col_name}",insert_sql)
        if bool(re.search("((substr|SUBSTR)\s*\([:a-zA-Z0-9_]+,\d+,\d+\))",insert_sql)):
            insert_sql=re.sub("((substr|SUBSTR)\s*\([:a-zA-Z0-9_]+,\d+,\d+\))",f":{col_name}",insert_sql)
        insert_sql = re.sub("(AS TIMESTAMP\(\d\)\)|as timestamp\(\d\)\))", "",insert_sql)
        insert_sql=re.sub("(CAST|cast|Cast)","",insert_sql)
        if insert_sql.startswith("("):
            insert_sql = insert_sql.replace("(", "to_timestamp(", 1)
    if bool(re.search("(AS DATE FORMAT|as date format|as date FORMAT|as DATE FORMAT)",insert_sql)):
        if bool(re.search("((trim|TRIM)\s*\([:a-zA-Z0-9_]+\))",insert_sql)):
            insert_sql=re.sub("((trim|TRIM)\s*\([:a-zA-Z0-9_]+\))",f":{col_name}",insert_sql)
        if bool(re.search("((substr|SUBSTR)\s*\([:a-zA-Z0-9_]+,\d+,\d+\))",insert_sql)):
            insert_sql=re.sub("((substr|SUBSTR)\s*\([:a-zA-Z0-9_]+,\d+,\d+\))",f":{col_name}",insert_sql)
        insert_sql = re.sub("(AS DATE FORMAT|as date format|as date FORMAT|as DATE FORMAT)", ",",insert_sql)
        insert_sql=re.sub("(CAST|cast|Cast)","to_date",insert_sql)
        if insert_sql.startswith("("):
            insert_sql=insert_sql.replace("(","to_date(",1)
    if bool(re.search("(AS TIME FORMAT|as time format|as time FORMAT|as TIME FORMAT)",insert_sql)):
        if bool(re.search("((trim|TRIM)\s*\([:a-zA-Z0-9_]+\))",insert_sql)):
            insert_sql=re.sub("((trim|TRIM)\s*\([:a-zA-Z0-9_]+\))",f":{col_name}",insert_sql)
        if bool(re.search("((substr|SUBSTR)\s*\([:a-zA-Z0-9_]+,\d+,\d+\))",insert_sql)):
            insert_sql=re.sub("((substr|SUBSTR)\s*\([:a-zA-Z0-9_]+,\d+,\d+\))",f":{col_name}",insert_sql)
        insert_sql = re.sub("(AS TIME FORMAT|as time format|as time FORMAT|as TIME FORMAT)", ",",insert_sql)
        insert_sql=re.sub("(CAST|cast|Cast)","to_date",insert_sql)
    if bool(re.search("(AS TIME\(\d\)|as time\(\d\))",insert_sql)):
        if bool(re.search("((trim|TRIM)\s*\([:a-zA-Z0-9_]+\))",insert_sql)):
            insert_sql=re.sub("((trim|TRIM)\s*\([:a-zA-Z0-9_]+\))",f":{col_name}",insert_sql)
        if bool(re.search("((substr|SUBSTR)\s*\([:a-zA-Z0-9_]+,\d+,\d+\))",insert_sql)):
            insert_sql=re.sub("((substr|SUBSTR)\s*\([:a-zA-Z0-9_]+,\d+,\d+\))",f":{col_name}",insert_sql)
        insert_sql = re.sub("( AS TIME\(\d\) FORMAT)", f",",insert_sql)
        insert_sql = re.sub("(AS TIME\(\d\))", f",hh:mi:ss", insert_sql)
        insert_sql=re.sub("(CAST|cast|Cast)","to_timestamp",insert_sql)
        if insert_sql.startswith("("):
            insert_sql=insert_sql.replace("(","to_timestamp(",1)
    insert_sql = insert_sql.replace("cast(", "").replace("''","'")
    insert_sql = insert_sql.replace("CAST(", "").replace("''", "'")
    if bool(re.search("(FORMAT|format|Format)", insert_sql)):
        insert_sql = re.sub("(FORMAT|format|Format)", ",", insert_sql)
    return insert_sql

def removeComments(string):
    string = re.sub(re.compile("/\*.*?\*/",re.DOTALL ) ,"" ,string) # remove all occurrences streamed comments (/*COMMENT */) from string
    string = re.sub(re.compile("//.*?\n" ) ,"" ,string) # remove all occurrence single-line comments (//COMMENT\n ) from string
    return string

def main():
    parser = argparse.ArgumentParser('Extract schema from Teradata loader scripts')
    parser.add_argument('--path_to_ctl_files', required=True, help='Pass the absolute path to the directory containing Fastload ctl files')
    parser.add_argument('--table_details_csv', required=True, help='Pass the absolute path to the table details csv file containing lrf path')
    args = vars(parser.parse_args())
    folder_path=args.get("path_to_ctl_files")
    table_details_csv_path=args.get("table_details_csv")
    if not os.path.exists(folder_path):
        print("Please enter a valid path to the folder containing the ctl files")
        exit(-100)
    if not os.path.exists(table_details_csv_path):
        print("Please enter a valid path to the csv file containing the lrf path details")
        exit(-100)
    result=[]
    files_in_current_directory=os.listdir(folder_path)
    table_details_df=pd.read_csv(table_details_csv_path)
    table_details_df['Table Name'].str.lower()
    file_properties = configuration_json["file_properties"]
    for filename in files_in_current_directory:
        table={}
        if filename.endswith("tpt.ctl"):
            print_warn(f"Parsing {filename}")
            s=''
            filepath=os.path.join(folder_path,filename)
            with open(filepath,"r") as f:
                s=f.read()
                s=re.sub("\n","",s)
                s=re.sub("\s+",' ',s)
            #print(s)
            filename=filepath.split("/")[-1].split(".")[0]
            filename=filename.split(".")[0]
            s=s.replace("\n","").replace("\r",'')
            schema_res = find_between(s,"DEFINE SCHEMA",");")
            #print("schema_res:",schema_res)
            schema_res = removeComments(schema_res)
            schema_res = schema_res.replace("\n","")
            layout_name=re.search("[a-zA-Z_]+\s*?\(",schema_res).group()
            layout_name=layout_name.replace("(","\(")
            #print("layout_name:", layout_name)
            #print(schema_res)
            schema_res = re.sub(layout_name,"",schema_res)
            print("schema_str before:",schema_res)
            #schema_res = schema_res.strip().strip("(").strip(")")
            #schema_str=schema_res.strip("(").strip(")").strip()
            schema_str=schema_res.strip()
            print("schema_str after:",schema_str)
            columns = [i.strip().split(" ")[0] for i in schema_str.split(",")]
            insert_sql=find_between(s,f"INSERT INTO ",");").strip()
            insert_sql = removeComments(insert_sql)
            insert_sql = f"INSERT INTO " + insert_sql + ");"

            #if insert_sql:
            #    insert_sql=remove_null_values_columns(insert_sql, filename)
            insert_sql=insert_sql.replace("''","\'")
            column_value_mappings,error=extract_tpt_columns(insert_sql)
            #print(column_value_mappings)
            parsing_error=""
            need_validation='No'
            if error != "":
                parsing_error=error
                need_validation = 'Yes'
            new_columns=list(column_value_mappings.keys())
            old_columns = list(column_value_mappings.values())
            #add_load_dt="No,"
            #add_updt_dt="No,"


            # for column in configuration_json.get("update_dt_columns",[]):
            #     if column in new_columns:
            #         idx=new_columns.index(column)
            #         new_columns.remove(column)
            #         add_updt_dt=','.join(['Yes',column])
            #         del old_columns[idx]
            #         column_value_mappings.pop(column)
            # for column in configuration_json.get("load_dt_columns",[]):
            #     if column in new_columns:
            #         idx=new_columns.index(column)
            #         new_columns.remove(column)
            #         add_load_dt=','.join(['Yes',column])
            #         del old_columns[idx]
            #         column_value_mappings.pop(column)
            #print("\nbefore:column_value_mappings",column_value_mappings)
            for k,v in column_value_mappings.items():
                column_value_mappings[k]=add_function_mappings(v)
            function_mappings={}
            column_mappings={}
            for k, v in column_value_mappings.items():
                regex_result = re.search(":.+?(?=(,|\s|\)))", v + ",")
                if regex_result:
                    column_mappings[regex_result.group().strip().strip(":")]=k
                else:
                    pass
            print("column_value_mappings:",column_value_mappings)
            new_columns=[]
            for k,v in column_value_mappings.items():
                v = re.sub(f"{k}\s*=\s*", "",v)
                col_name = re.search(":.+?(?=(,|\s|\)))", v + ",")
                if not col_name:
                    new_columns.append({k:v})
                    continue
                else:
                    col_name = col_name.group().strip().strip(":")
                col_name=column_mappings[col_name]
                if not v.startswith(":"):
                    # if v.lower().replace(" ","").startswith("trim(:"):
                    #     function_mappings[col_name]="trim"
                    # elif v.lower().replace(" ","").startswith("substr(:"):
                    #     start=v.split(",")[1]
                    #     end=v.split(",")[2]
                    #     function_mappings[col_name] = f"substr({start},{end})"
                    if v.lower().replace(" ","").startswith("to_date"):
                        v=re.sub("(TRIM\([a-zA-Z\:0-9_]+\)|trim\([a-zA-Z\:0-9_]+\))","",v)
                        v = re.sub("(SUBSTR\([a-zA-Z\:0-9_]+\)|substr\([a-zA-Z\:0-9_]+\))", "", v)
                        format = v.split(",")[-1]
                        function_mappings[col_name] = f"to_date({format})"
                    elif v.lower().replace(" ","").startswith("to_timestamp"):
                        v=re.sub("(TRIM\([a-zA-Z\:0-9_]\)|trim\([a-zA-Z\:0-9_]\))","",v)
                        v = re.sub("(SUBSTR\([a-zA-Z\:0-9_ ]+,\s*\d+\s*,\s*\d+\s*\)|substr\([a-zA-Z\:0-9_ ]+,\s*\d+\s*,\s*\d+\s*\))", "", v)
                        timestamp_nested_count=v.count("to_timestamp")
                        format = v.split(",")[-timestamp_nested_count]
                        if format.startswith('to_timestamp'):
                            format=""
                        function_mappings[col_name] = f"to_timestamp({format})"
                    elif v.lower().replace(" ","").startswith(":"):
                        function_mappings[col_name] = v
                    elif "casewhen" in v.lower().replace(" ",""):
                        function_mappings[col_name] = v
                        need_validation='Yes'
                    else:
                        function_mappings[col_name] = v
                        #need_validation = 'Yes'
                else:
                    pass

            for k,v in function_mappings.items():
                v = re.sub(f"{k}\s*=\s*", "",v)
                function_mappings[k]=v.replace("((","(").replace("))",")").replace("'","").replace("\"","").replace(" ","")

            print("function_mappings:",function_mappings)

            #need_validation = 'Yes' if bool(re.search("(case when\s*\(|CASE WHEN\s*\()",insert_sql)) else 'No'
            print("\n")
            print_warn("LRF Schema: ")
            print(columns)
            print_warn("Target Columns: ")
            print(new_columns)
            print_warn("Column Rename Mappings: ")
            print(column_mappings)
            print_warn("Column Function Mappings: ")
            print(function_mappings)
            print_success(f"Done parsing {filename}")
            lrf_schema=','.join(columns)
            mappings=json.dumps(column_mappings)
            #mappings="\""+mappings+"\""
            #csv_file.write(f"{filename}|{lrf_schema}|{mappings}|{need_validation}\n")
            table["TABLE_NAME"]=filename
            table["LRF_SCHEMA"]=lrf_schema
            table["TABLE_SCHEMA_MAPPINGS"]=mappings
            table["NEED_VALIDATION"]=need_validation
            table["PARSING_ERROR"]=parsing_error
            # table["ADD_UPDT_DT"]=add_updt_dt
            # table["ADD_LOAD_DT"]=add_load_dt
            table["NEW_COLUMNS"]=json.dumps(new_columns)
            table["FUNCTION_MAPPINGS"]=json.dumps(function_mappings)
            #table["EXTRA_COLUMNS_THAN_LRF"]= ','.join(extra_columns)
            table["FILE_PROPERTIES"]=json.dumps(file_properties)
            result.append(table)
        else:
            pass
    print(f"Writing the result to csv {cwd}/table_schema.csv ")
    resultant_df = pd.DataFrame(result)
    resultant_df=pd.merge(resultant_df, table_details_df,left_on="TABLE_NAME", right_on="Table Name", how="inner")
    resultant_df.drop("Table Name",axis=1,inplace=True)
    resultant_df.to_csv("./table_schema.csv", index=False)
    print("Wrote successfully!")

if __name__ == '__main__':
    main()