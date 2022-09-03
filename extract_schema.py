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
    columns = find_between(insert_sql, f"INSERT INTO {filename}", "VALUES").strip()
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
    insert_sql=re.sub("(DECIMAL(\d+,\d+))","",insert_sql)
    col_name = re.search(":.+?(?=(,|\s|\)))", insert_sql + ",")
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


def main():
    parser = argparse.ArgumentParser('Extract schema from Teradata loader scripts')
    parser.add_argument('--path_to_ctl_files', required=True, help='Pass the absolute path to the directory containing Fastload ctl files')
    args = vars(parser.parse_args())
    folder_path=args.get("path_to_ctl_files")
    cwd=os.getcwd()
    if not os.path.exists(folder_path):
        print("Please enter a valid path to the folder containing the ctl files")
        exit(-100)
    result=[]
    files_in_current_directory=os.listdir(folder_path)
    for filename in files_in_current_directory:
        table={}
        if filename.endswith("tpt.ctl"):
            print_warn(f"Parsing {filename}")
            s=''
            filepath=filename
            with open(filepath,"r") as f:
                s=f.read()
                s=re.sub("\s+",' ',s)
            #print(s)
            filename=filepath.split("/")[-1].split(".")[0]
            filename=filename.split(".")[0]
            s=s.replace("\n","").replace("\r",'')
            schema_res = find_between(s,"DEFINE SCHEMA INPUTLAYOUT",");")
            print("schema_str before:",schema_res)
            schema_res = schema_res.strip().strip("(").strip(")")
            schema_str=schema_res.strip("(").strip(")").strip()
            print("schema_str after:",schema_str)
            columns = [i.strip().split(" ")[0] for i in schema_str.split(",")]
            insert_sql=find_between(s,f"INSERT INTO {filename}",");").strip()
            insert_sql = f"INSERT INTO {filename} " + insert_sql + ");"
            insert_sql=remove_null_values_columns(insert_sql, filename)
            insert_sql=insert_sql.replace("''","\'")
            column_value_mappings,error=extract_tpt_columns(insert_sql)
            parsing_error=""
            need_validation='No'
            if error != "":
                parsing_error=error
            new_columns=list(column_value_mappings.keys())
            old_columns = list(column_value_mappings.values())
            add_load_dt='No'
            add_updt_dt='No'
            if "updt_dt" in new_columns:
                idx=new_columns.index('updt_dt')
                new_columns.remove("updt_dt")
                add_updt_dt='Yes'
                del old_columns[idx]
                column_value_mappings.pop("updt_dt")
            elif "Updt_dt" in new_columns:
                idx = new_columns.index('Updt_dt')
                new_columns.remove("Updt_dt")
                add_updt_dt='Yes'
                del old_columns[idx]
                column_value_mappings.pop("Updt_dt")
            if "load_dt" in new_columns:
                idx = new_columns.index('load_dt')
                new_columns.remove("load_dt")
                add_load_dt='Yes'
                del old_columns[idx]
                column_value_mappings.pop("load_dt")
            elif "Load_dt" in new_columns:
                idx = new_columns.index('Load_dt')
                new_columns.remove("Load_dt")
                add_load_dt='Yes'
                del old_columns[idx]
                column_value_mappings.pop("Load_dt")
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
            for k,v in column_value_mappings.items():
                col_name = re.search(":.+?(?=(,|\s|\)))", v + ",")
                col_name = col_name.group().strip().strip(":")
                col_name=column_mappings[col_name]
                if not v.startswith(":"):
                    if v.lower().replace(" ","").startswith("trim(:"):
                        function_mappings[col_name]="trim"
                    elif v.lower().replace(" ","").startswith("substr(:"):
                        start=v.split(",")[1]
                        end=v.split(",")[2]
                        function_mappings[col_name] = f"substr({start},{end})"
                    elif v.lower().replace(" ","").startswith("to_date"):
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
                    else:
                        function_mappings[col_name] = v
                        need_validation='Yes'
                else:
                    pass

            for k,v in function_mappings.items():
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
            table["ADD_UPDT_DT"]=add_updt_dt
            table["ADD_LOAD_DT"]=add_load_dt
            table["FUNCTION_MAPPINGS"]=json.dumps(function_mappings)
            #table["EXTRA_COLUMNS_THAN_LRF"]= ','.join(extra_columns)
            result.append(table)
        else:
            pass
    print(f"Writing the result to csv {cwd}/table_schema.csv ")
    resultant_df = pd.DataFrame(result)
    resultant_df.to_csv("./table_schema.csv", index=False)
    print("Wrote successfully!")

if __name__ == '__main__':
    main()