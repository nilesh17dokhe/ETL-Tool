import builtins
import datetime
import json
import math
import os
import statistics
import re
from datetime import date
from datetime import timedelta
from dateutil import parser
from dateutil.parser import parse
from fractions import Fraction

import numpy
import pandas as pd
import numpy as np
import dask.dataframe as dd
from dateutil import parser
from decimal import Decimal, ROUND_HALF_UP

from PACE_ETL.Models.ResultModel import Result
from PACE_ETL.Services.FileService import FileService
from PACE_ETL.Services.FormulaEngine import FormulaParserService
from PACE_ETL.Services.GenericServices import GenericService
from PACE_ETL.GlobalConfig import GlobalConfig
from zoneinfo import ZoneInfo,available_timezones
is_add_column = False
expDict = {}
ascii = 65
formulaArray = []
stringFormulas = ["CLEAN","LTRIM","RTRIM","FIND","LEFT","LEN","LOWER","MID","REPT","RIGHT","SUBSTITUTE","TRIM",
                  "UPPER", "CONCAT", "TEXT", "SEARCH"]
MathFormulas = ["AVERAGE","CEILING","EXP","POWER","ROUNDVAL","SUMIFS","ABS","AVERAGEIF","COUNT","COUNTIF","COUNTIFS",
                "FACTDOUBLE","FACT","FLOOR","SQRT","SUM","SUMIF","RANK","AVERAGEIFS","SUMOFF","SUMFIX", "MIN", "MAX"]
dateFormulas = ["DATE", "TODAY", "NOW", "DATEVALUE", "DAY", "MONTH", "HOUR", "MINUTE", "YEAR", "EOMONTH", "DATEDIFF", "WEEKDAY"]
StatisticalFormulas = ["MEAN","MODE","MEDIAN","STDEV","CORREL"]
otherFormulas = ["IF","MATCH","INDEX","XLOOKUP","VLOOKUP","LOOKUP", "ISNUMBER", "ISBLANK"]
conditionalFormulas = ["IFERROR","OR","AND"]
formulaList = stringFormulas + MathFormulas + otherFormulas + conditionalFormulas + StatisticalFormulas+dateFormulas


def getChar(index):
    try:
        result = ""
        while index >= 0:
            result = chr(index % 26 + 65) + result
            index = index // 26 - 1
            if index < 0:
                break
        return result
    except Exception as ex:
        raise Exception(str(ex))


def extract_columns(formula: str, dfObj: str,app_config):
    try:
        file_service = FileService()
        columns =[]
        columnList = file_service.get_columnName_from_format(dfObj["DataSourceId"],app_config)
        formula = formula.lower();
        for column in columnList:
            stringtoCheck = "["+dfObj["DataModelName"].lower()+"."+column.lower()+"]"
            if stringtoCheck in formula:
                columns.append(column)
        return columns
    except Exception as ex:
        raise Exception(ex)
def evaluateExpression(expression, dataModelFileMapping, currentDf,currentDataModel,newColumnName, s3_obj, app_config, isMeasure,user_timezone):
    try:
        global isToNumeric
        isToNumeric = True
        expresult = None
        expression = expression[1:]
        global globaldf
        expression = str(expression).strip()
        global ascii
        global formulaArray
        ascii = 0
        tempexpression = expression
        gen_serv_obj = GenericService()
        file_service = FileService()
        # creating dataframes which are included in formula.
        dataframes = {}
        rename_mapping = {}
        use_col_mapping = {}

        if expression in formulaList:
            raise ValueError("Invalid Formula")

        for dfObj in dataModelFileMapping:
            use_col_mapping[dfObj['DataModelName']] = extract_columns(expression, dfObj,app_config)

        for dfObj in dataModelFileMapping:
            try:
                df = file_service.get_dataframe(dfObj['DataSourceId'], dfObj['Delimeter'], dfObj['Encoding'],
                                                app_config, use_col_mapping[dfObj['DataModelName']])
                dataframes[dfObj['DataModelName']] = df.compute().astype("O").replace({pd.NA: np.nan})
                rename_mapping[dfObj['DataModelName']] = {}
            except pd.errors.EmptyDataError:
                df = pd.DataFrame()

        # Parsing formula to work with ast
        mapping = {}
        dfMapping = {}
        group_mapping = {}
        for df_name, data in dataframes.items():
            # Replacing @Column names with ascii values
            for column in data.columns:
                if "@[" + df_name + "." + column + "]" in expression:
                    group_mapping["@_" + getChar(ascii) + "_"] = {"df_name": df_name, "col_name": column}
                    expression = expression.replace("@[" + df_name + "." + column + "]", "_" + getChar(ascii) + "_")
                    ascii += 1

            if "["+df_name+"]" in expression:
                dfMapping["_" + getChar(ascii) + "_"] = {"df_name": df_name}
                expression = expression.replace("[" + df_name + "]", "_" + getChar(ascii) + "_")
                ascii += 1

            # Replacing column names with ascii values.
            for column in data.columns:
                if "[" + df_name + "." + column + "]" in expression:
                    mapping["_" + getChar(ascii) + "_"] = {"df_name": df_name, "col_name": column}
                    rename_mapping[df_name][column] = "_" + getChar(ascii) + "_"
                    expression = expression.replace("[" + df_name + "." + column + "]", "_" + getChar(ascii) + "_")
                    ascii += 1
        # check for use of any non-existent column in formula.
        df_list = list(dataframes.keys())
        # patterns = [r'\[' + re.escape(ds) + r'\.([^\]]+?)\](?!\])' for ds in df_list]
        patterns = [rf'\[{re.escape(ds)}\.(.+?)\]' for ds in df_list]
        combined_pattern = '|'.join(patterns)
        matches = re.findall(combined_pattern, expression)
        flattened_matches = [item for sublist in matches for item in (sublist if isinstance(sublist, tuple) else [sublist])]
        matches = [match for match in flattened_matches if match]
        if matches:
            raise Exception(", ".join(matches) + " column/s not present in data models.")

        # Parsing formula expression.
        formula_list = FormulaParserService.ParseFormula(expression)

        formula_list.reverse()
        result = None
        next_formula = ""
        isLast = False
        exec_status = []
        finalDict = {}

        # Replacing ascii with actual
        for i, string in enumerate(formula_list):
            # Replacing ascii with actual from group mapping
            for key, value in group_mapping.items():
                formula_list[i] = formula_list[i].replace(key[1:], f'dataframes["{value["df_name"]}"]["{value["col_name"]}"].name')

            for key, value in mapping.items():
                formula_list[i] = formula_list[i].replace(key, f'dataframes["{value["df_name"]}"]["{key}"]')

            for key, value in dfMapping.items():
                formula_list[i] = formula_list[i].replace(key, f'dataframes["{value["df_name"]}"]')

        # Renaming columns in df which are used in formula.
        for df_name, mapping in rename_mapping.items():
            dataframes[df_name].rename(columns=mapping, inplace=True)

        # Executing each parsed expression from list
        count = 0
        popList = []
        for i, formula in enumerate(formula_list):
            originalFormula = formula
            for j, status in enumerate(exec_status):
                if status['Formula'] in formula:
                    formula = formula.replace(status["Formula"], "finalDict['result"+str(count)+"']")
                    finalDict["result"+str(count)] = status["Result"]
                    popList.append(status)
                    count = count+1

            for status in popList:
                exec_status.remove(status)

            popList = []

            if i == len(formula_list) - 1:
                isLast = True

            formula = uppercase_formula(formula)
            if "NOW()" in formula:
                formula = formula.replace("NOW()", f"'{NOW(user_timezone)}'")
            # Evaluating IF ERROR
            if formula.startswith("IFERROR"):
                arguments = formula[formula.index('(') + 1: formula.rindex(')')].split(',')
                arguments = [arg.strip() for arg in arguments]
                args = {
                    "condition": arguments[0],
                    "default_value": arguments[-1].strip('"').strip("'"),
                    "finalDict": finalDict,
                    "dataframes": dataframes
                }
                func_obj = eval("IFERROR")
                expresult = func_obj(**args)

            elif formula.upper().startswith("IF"):
                args = {
                    "formula": formula,
                    "df": df,
                    "finalDict": finalDict,
                    "dataframes": dataframes
                }
                func_obj = eval("IF1")
                expresult = func_obj(**args)
            elif formula.upper().startswith("AND") or formula.upper().startswith("OR"):
                args = {
                    "formula": formula,
                    # "df": df,
                    "finalDict": finalDict,
                    "dataframes": dataframes
                }
                func_obj = eval("AND1")
                expresult = func_obj(**args)
            elif formula.upper().startswith("SUMIFS") or formula.upper().startswith("SUMIF"):
                args = {
                    "formula": formula,
                    "finalDict": finalDict,
                    "dataframes": dataframes,
                    "currentDf":currentDf,
                    "currentDataModel": currentDataModel

                }
                func_obj = eval("SUMIFS")
                expresult = func_obj(**args)
            elif formula.upper().startswith("COUNTIFS") or formula.upper().startswith("COUNTIF"):
                args = {
                    "formula": formula,
                    "finalDict": finalDict,
                    "dataframes": dataframes,
                    "currentDf": currentDf,
                    "currentDataModel":currentDataModel

                }
                func_obj = eval("COUNTIFS")
                expresult = func_obj(**args)
            elif formula.upper().startswith("AVERAGEIFS") or formula.upper().startswith("AVERAGEIF"):
                args = {
                    "formula": formula,
                    "finalDict": finalDict,
                    "dataframes": dataframes,
                    "currentDf": currentDf,
                    "currentDataModel": currentDataModel

                }
                func_obj = eval("AVERAGEIFS")
                expresult = func_obj(**args)
            elif formula.upper().startswith("SUMFIX"):
                args = {
                    "formula": formula,
                    "finalDict": finalDict,
                    "dataframes": dataframes,
                    "currentDf": currentDf,
                }
                func_obj = eval("SUMFIX")
                expresult = func_obj(**args)
            # Evaluating Others
            else:
                expresult = eval(formula, locals(), globals())

            # Convert ndarray type to Series.
            if isinstance(expresult, (np.ndarray, list, pd.Series)):
                expresult = pd.Series(expresult)
                # expresult = dd.from_pandas(expresult, npartitions=currentDf.npartitions)
                expresult = expresult.replace('', np.nan)
                expresult = expresult.replace("nan", None)

            # if is_convertible(expresult) and isToNumeric:
            if isMeasure and is_convertible(expresult):
                expresult = pd.to_numeric(expresult, errors='ignore')

            # Creating dict to maintain executed formula results
            status = {
                "Formula": originalFormula,
                "Result": expresult,
                "isReplaced": False
            }
            exec_status.append(status)
            if isinstance(expresult, pd.Series):
                expresult = expresult.replace("nan", None)

        # Returning final result
        swapped_mapping = {value: key for key, value in mapping.items()}
        temp_expr = formula_list[-1]
        for dfObj in dataModelFileMapping:
                # editPath = dfObj["FilePath"].rstrip('.txt') + "_EditMetaData"
                editPath = f"{app_config.ETL_PATH}{dfObj['DataSourceId']}/LastUpdatedFiles/{dfObj['DataSourceId']}_EditMetaData"
                if GlobalConfig.IS_WEB:
                    editjson = json.loads(s3_obj.get_text_from_s3(editPath))
                else:
                    with open(editPath, 'r') as file:
                         editjson = file.read()
                         editjson = json.loads(editjson)
                for column in dataframes[dfObj['DataModelName']].columns:
                    if 'dataframes["' + dfObj['DataModelName'] + '"]["' + column + '"]' in temp_expr:
                        column = swapped_mapping.get(column) if swapped_mapping.get(column) is not None else column
                        obj = {
                            # "DataModelName": dfObj['DataModelName'],
                            "DataModelName": currentDataModel,
                            "ColumnName": newColumnName
                        }

                        column_names = [item.get("ColumnName") for item in editjson]
                        for object in editjson:
                            if object.get("ColumnName") == column:
                                if find_object_by_two_keys(object["Dependent"],dfObj['DataModelName'],newColumnName) == None:
                                    object["Dependent"].append(obj)
                        if GlobalConfig.IS_WEB:
                            s3_obj.put_text_to_s3(json.dumps(editjson), editPath)
                        else:
                            file_obj = open(editPath, 'w', encoding="utf-8")
                            json.dump(editjson, file_obj, indent=4)
                            file_obj.close()
        result = Result(1, "Column Added", expresult)
        return result
    except SyntaxError as ex:
        raise SyntaxError("Invalid Formula")
    except Exception as ex:
        raise Exception(str(ex))


def uppercase_formula(input_string):
    index_of_parenthesis = input_string.find('(')
    # If '(' is not found, return the string as is
    if index_of_parenthesis == -1:
        return input_string
    # Extract the part before '(' and after '('
    before_parenthesis = input_string[:index_of_parenthesis]
    after_parenthesis = input_string[index_of_parenthesis:]
    # Convert the part before '(' to uppercase
    if before_parenthesis.upper() in formulaList:
        before_parenthesis = before_parenthesis.upper()
        # Combine the uppercase part with the rest of the string
    result = before_parenthesis + after_parenthesis
    return result


def find_object_by_two_keys(objects,DataModelName,ColumnName):
    for item in objects:
            if item["ColumnName"] == ColumnName and item["DataModelName"] == DataModelName:
                return item
    return None


def is_convertible(series):
    try:
        if isinstance(series, pd.Series) and series.dtype != 'datetime64[ns]':
            pd.to_numeric(series, errors='raise')
            return True
        return False
    # except ValueError:
    except Exception:
        return False


def IF1(formula, df, finalDict, dataframes):
    try:
        formula = parse_if_expression(formula, df, finalDict, dataframes)
        result = eval(formula, locals(), globals())
        if is_convertible(result):
            expresult = pd.to_numeric(result, errors='ignore')
        return result
    except Exception as ex:
        raise Exception(str(ex))


def parse_if_expression(formula, df, finalDict, dataframes):
    try:
        expression = formula[formula.index('(') + 1: formula.rindex(')')]

        pattern = re.compile(r'''((?:[^',]|'[^']*')+)''')
        segments = pattern.findall(expression)
        segments = [segment.strip() for segment in segments]
        arguments = segments

        pattern = r"(.+?)\s*([=!<>]+)\s*'([^']*)'"
        # pattern = r"(.+?)\s*([<>]=|!=|==|[<>])\s*(?:['\"]?([^'\"]*)['\"]?)"

        # Using regex to split the string
        condition_match = re.match(pattern, arguments[0])

        updated_cond = arguments[0]
        if condition_match:
            column = condition_match.group(1)
            operator = condition_match.group(2).strip()
            value = condition_match.group(3)

            if column.startswith("dataframes[") or column.startswith("finalDict['result"):
                col_val = eval(column, locals(), globals())

                if isinstance(col_val, pd.Series) and col_val.dtype == 'object':
                    # if value.isnumeric():
                    #     col_val = pd.to_numeric(col_val, errors="coerce")
                    #     numeric_update = f'{column} = col_val'
                    #     exec(numeric_update)
                    #     updated_cond = re.sub(rf"'{re.escape(value)}'", f"{value.upper()}", updated_cond)
                    if value != "":
                        updated_cond = re.sub(rf"'{re.escape(value)}'", f"'{value.upper()}'", updated_cond)
                        updated_cond = updated_cond.replace(column, column + ".str.upper()")
                        # updated_cond = updated_cond.replace(value, value.upper())
                    elif value == "":
                        updated_cond = f"pd.isna({column})" if operator == "==" else f"~pd.isna({column})"
                elif isinstance(col_val, pd.Series) and pd.api.types.is_numeric_dtype(col_val.dtype):
                    if value == "":
                        updated_cond = f"pd.isna({column})" if operator == "==" else f"~pd.isna({column})"
            else:
                # Replacing values for static values
                updated_cond = re.sub(rf"'{re.escape(value)}'", f"'{value.upper()}'", updated_cond)
                updated_cond = updated_cond.replace(column, column.upper())
                # updated_cond = updated_cond.replace(value, value.upper())

        formula = formula.replace(arguments[0], updated_cond)
        return formula
    except Exception as ex:
        raise Exception(str(ex))


def IF(condition, true_block, false_block=False):
    try:
        if isinstance(condition, (pd.Series, list, tuple)):
            # result = [true_block if cond else false_block for cond in condition]
            result = np.where(condition, true_block, false_block)
            return result
        else:
            return true_block if condition else false_block
    except Exception as ex:
        raise Exception(ex)


def convert_to_number_or_keep_as_is(s):
    try:
        return int(s) if float(s).is_integer() else float(s)
    except ValueError:
        return s


def IFS(*args):
    try:
        if len(args) % 2 != 0:
            raise ValueError("Number of arguments must be even.")

        condition_list = []
        value_list = []
        for i in range(0, len(args), 2):
            condition, value = args[i], args[i + 1]
            condition_list.append(pd.Series(condition))
            value_list.append(value)
    except Exception as ex:
        raise Exception(ex)


def IFERROR(condition, default_value,dataframes,finalDict):
    try:
        combined_df = concat_dataframes(dataframes)
        for key,value in dataframes.items():
            for col in value.columns:
                condition = condition.replace(f'dataframes["{key}"]["{col}"]',f'{key}_{col}')
        result = (
            combined_df.apply(lambda row: evaluate_iferror(row, condition, default_value), axis=1))
        return result
    except Exception as ex:
        raise Exception(ex)


def replace_column_names(expr, row_dict):
    # Sort column names by length to avoid partial replacements
    for col in sorted(row_dict.keys(), key=len, reverse=True):
        # Use regex to find and replace column names with `row_dict['column_name']`
        expr = re.sub(r'\b{}\b'.format(re.escape(col)), f"row_dict['{col}']", expr)
    return expr


def evaluate_iferror(row, expression, default_value):
    try:
        row_dict = row.to_dict()
        safe_expression = replace_column_names(expression, row_dict)
        # Use eval with the row dictionary as context
        result = eval(safe_expression,locals(),globals())
        #result = eval(expression, globals(), locals())
        final_result = result if not pd.isna(result) else get_default_value(row.name,default_value)
        return final_result
    except Exception as e:
        print(f"Error: {e}")
        return get_default_value(row.name, default_value)


def get_default_value(index, default_value):
    if isinstance(default_value, pd.Series):
        return default_value.loc[index]
    else:
        return default_value


def AND1(formula, finalDict, dataframes):
    try:
        formula = parse_and_expression(formula, finalDict, dataframes)
        result = eval(formula, locals(), globals())
        if is_convertible(result):
            expresult = pd.to_numeric(result, errors='ignore')
        return result
    except Exception as ex:
        raise Exception(str(ex))


def parse_and_expression(formula, finalDict, dataframes):
    try:
        expression = formula[formula.index('(') + 1: formula.rindex(')')]

        pattern = re.compile(r'''((?:[^',]|'[^']*')+)''')
        segments = pattern.findall(expression)
        segments = [segment.strip() for segment in segments]
        arguments = segments

        pattern = r"(.+?)\s*([=!<>]+)\s*'([^']*)'"

        for arg in arguments:
            # Using regex to split the string
            condition_match = re.match(pattern, arg)

            updated_cond = arg
            if condition_match:
                column = condition_match.group(1)
                operator = condition_match.group(2).strip()
                value = condition_match.group(3)

                if column.startswith("dataframes[") or column.startswith("finalDict['result"):
                    col_val = eval(column, locals(), globals())

                    if isinstance(col_val, pd.Series) and col_val.dtype == 'object':
                        if value != "":
                            updated_cond = re.sub(rf"'{re.escape(value)}'", f"'{value.upper()}'", updated_cond)
                            updated_cond = updated_cond.replace(column, column + ".str.upper()")
                            # updated_cond = updated_cond.replace(value, value.upper())
                        elif value == "":
                            updated_cond = f"pd.isna({column})" if operator == "==" else f"~pd.isna({column})"
                    elif isinstance(col_val, pd.Series) and pd.api.types.is_numeric_dtype(col_val.dtype):
                        if value == "":
                            updated_cond = f"pd.isna({column})" if operator == "==" else f"~pd.isna({column})"
                else:
                    # Replacing values for static values
                    updated_cond = re.sub(rf"'{re.escape(value)}'", f"'{value.upper()}'", updated_cond)
                    updated_cond = updated_cond.replace(column, column.upper())
                    # updated_cond = updated_cond.replace(value, value.upper())
            formula = formula.replace(arg, updated_cond)
        return formula
    except Exception as ex:
        raise Exception(str(ex))


def AND(*args):
    try:
        result = args[0]
        result = to_boolean(result)
        for series in args[1:]:
            result &= to_boolean(series)
        return result
    except Exception as ex:
        raise Exception(ex)


def OR(*args):
    try:
        result = args[0]
        result = to_boolean(result)
        for series in args[1:]:
            result |= to_boolean(series)
        return result
    except Exception as ex:
        raise Exception(ex)


# Function to convert values to boolean
def to_boolean(series):
    try:
        if pd.api.types.is_numeric_dtype(series):
            return series.astype(bool)  # Non-zero numbers are True, zero is False
        elif pd.api.types.is_object_dtype(series):
            # Handle string booleans
            non_blank_values = series.str.upper().replace('', pd.NA).dropna()

            # Check if all non-blank values are "TRUE" or "FALSE"
            if non_blank_values.isin(['TRUE', 'FALSE']).all():
                # Convert string boolean representations to actual booleans
                return series.str.upper().map({'TRUE': True, 'FALSE': False}).fillna(False)
            else:
                # raise ValueError("Text column contains non-boolean values")
                return series.apply(update_values)
        elif isinstance(series, (int, float)):
            return bool(series)  # Scalar numeric values, 0 = False, non-zero = True
        elif isinstance(series, str):
            value_upper = series.upper()
            if value_upper == "TRUE":
                return True
            elif value_upper == "FALSE":
                return False
            else:
                # raise ValueError(f"Invalid boolean string: {series}")
                return True
        else:
            raise ValueError("Invalid inputs!")  # Default conversion for other types
    except Exception as ex:
        raise Exception(ex)


def update_values(val):
    # Handle blank or None values as False
    if pd.isna(val) or val == '':
        return True
    try:
        numeric_val = float(val)
        # Return False only if it's zero, otherwise True
        return numeric_val != 0
    except ValueError:
        # Non-numeric strings are treated as True
        return True


def replaceAssciwithActuals(expression):
    try:
        global formulaArray
        for formula in reversed(formulaArray):
            keys = formula.keys()
            for key in keys:
                expression = expression.replace(key, formula[key])
           # expression.replace(formula.key(),formula.value())
        return expression
    except Exception as ex:
        raise Exception(ex)


def replaceColumnNameswithBacktick(expression, columns):
    try:
        mapping = []
        ascii = 65
        columns = sorted(columns, key=lambda x: len(x),reverse=True)
        for column in columns:
            if column in expression:
                dict={
                    "~"+str(ascii)+"~":column
                }
                expression = expression.replace(column, '`' + "~"+str(ascii)+"~" + '`')
                ascii = ascii+1
                mapping.append(dict)
        for obj in mapping:
            for key,val in obj.items():
                expression = expression.replace(key,val)

        for column in columns:
            newname = "@"+"`"+column+"`"
            name ='"@'+column+'"'
            expression = expression.replace(newname,name)
        return expression
    except Exception as ex:
       raise Exception(str(ex))


def replaceColumnNamesWithBackTick(expression, columns):
    try:
        for column in columns:
            if column in expression:
                expression = expression.replace(column,"`"+column+"`")
        return expression
    except Exception as ex:
       raise Exception(str(ex))


def replaceColumnNames(expression, columns):
    try:
        mapping = []
        ascii = 65
        columns = sorted(columns, key=lambda x: len(x),reverse=True)
        for column in columns:
            if column in expression:
                dict = {
                    "~" + str(ascii) + "~": 'df["' + column + '"]'
                }
                expression = expression.replace(column, "~" + str(ascii) + "~" )
                ascii = ascii + 1
                mapping.append(dict)
        for obj in mapping:
            for key, val in obj.items():
                expression = expression.replace(key, val)
        return expression
        #
        #
        # for column in columns:
        #     if column in expression:
        #         expression = expression.replace(column, 'df["' + column + '"]')
        # return expression
    except Exception as ex:
       raise Exception(str(ex))


def replaceValuesForNestedFormula(expression):
    try:
        global expDict
        global ascii
        comparisionOperatorList = ["<", ">", "<=", ">=", "==", "!="]
        opencount = expression.count("(")
        closecount = expression.count(")")
        if opencount == 1 and closecount == 1:
            return expression
        else:
            closingindex = expression.find(")")
            tempList = []
            openingBrackdetIndex = 0
            for element in range(closingindex, 0, -1):
                if expression[element] == "(":
                    tempList.append(expression[element])
                    openingBrackdetIndex = element
                    break
                tempList.append(expression[element])
            tempExpression = []
            for element in range(openingBrackdetIndex - 1, 0, -1):
                if expression[element] == "=" and expression[element - 1] in comparisionOperatorList:
                    break
                if expression[element] == "," or expression[element] == "(" or expression[
                    element] in comparisionOperatorList:
                    break
                tempExpression.append(expression[element])
            reversedFormulaExpression = tempExpression[::-1]
            formulaExpression = "".join(reversedFormulaExpression)
            reversed = tempList[::-1]
            subExpression = "".join(reversed)
            if formulaExpression in formulaList:
                subExpression = formulaExpression + subExpression
                expression = expression.replace(subExpression, chr(ascii))
                expDict[chr(ascii)] = subExpression
                ascii = ascii + 1
            else:

                expressionResult = eval(subExpression)
                expression = expression.replace(subExpression, str(expressionResult))
            return replaceValuesForNestedFormula(expression)

    except Exception as ex:
        raise Exception(str(ex))


def checkisValidparentheses(expression):
    try:
        stack = []
        open_list = ["[", "{", "("]
        close_list = ["]", "}", ")"]
        for i in expression:
            if i in open_list:
                stack.append(i)
            elif i in close_list:
                pos = close_list.index(i)
                if ((len(stack) > 0) and
                        (open_list[pos] == stack[len(stack) - 1])):
                    stack.pop()
                else:
                    return 0
        if len(stack) == 0:
            return 1
        else:
            return 0
    except Exception as ex:
        raise Exception(str(ex))


# string functions
def LEN(value):
    try:
        if isinstance(value, str):
            return len(value)

        elif isinstance(value, (int, float)):
            return len(str(int(value))) if float(value).is_integer() else len(str(value))

        elif isinstance(value, (datetime.datetime, date)):
            return len(str(value))

        elif isinstance(value, pd.Series):
            return value.apply(lambda x: LEN(x) if pd.notna(x) else 0)
        else:
            return value
    except Exception as ex:
        raise Exception(str(ex))


def UPPER(value):
    try:
        if isinstance(value, str):
            return value.upper()
        elif isinstance(value, list):
            return list(map(lambda x: str(x).upper(), value))
        elif isinstance(value, pd.Series):
            return value.map(lambda x: str(x).upper() if not pd.isna(x) else np.nan)
        else:
            return value
    except Exception as ex:
        raise Exception(str(ex))


def LOWER(value):
    try:
        if isinstance(value, str):
            return value.lower()
        elif isinstance(value, list):
            return list(map(lambda x: str(x).lower(), value))
        elif isinstance(value, pd.Series):
            return value.map(lambda x: str(x).lower() if not pd.isna(x) else np.nan)
        else:
            return value
    except Exception as ex:
        raise Exception(str(ex))


def LEFT(key, amount):
    try:
        if isinstance(key, pd.Series) and isinstance(amount, pd.Series):
            return pd.Series([
                LEFT(k, a) if not pd.isna(k) and not pd.isna(a) else np.nan
                for k, a in zip(key, amount)
            ])
        elif isinstance(key, str):
            return key[:amount]
        # elif type(key) == list:
        #     return list(map(lambda x: str(x)[:amount]))
        elif isinstance(key, (int, float)):
            return str(int(key))[:amount] if float(key).is_integer() else str(key)[:amount]
        elif isinstance(key, pd.Series):
            return key.apply(lambda x: LEFT(x, amount) if not pd.isna(x) else np.nan)
        else:
            return key
    except Exception as ex:
        raise Exception(str(ex))


# def LEFT(key, amount):
#     try:
#         # Validation for scalar amount
#         if isinstance(amount, (int, float)):
#             if amount < 0:
#                 raise ValueError("Amount cannot be negative.")
#
#         # Validation for amount if it's a pandas Series
#         elif isinstance(amount, pd.Series):
#             if not np.issubdtype(amount.dtype, np.number):
#                 raise ValueError("Amount must contain only numeric values.")
#             if any(amount < 0):
#                 raise ValueError("Amount cannot contain negative values.")
#         else:
#             raise ValueError("Invalid amount value.")
#
#         # Handle cases based on the type of 'key'
#         if isinstance(key, str):
#             # If key is a single string and amount is a scalar
#             return key[:int(amount)]
#         elif isinstance(key, list):
#             # If key is a list and amount is scalar
#             if isinstance(amount, (int, float)):
#                 return list(map(lambda x: str(x)[:int(amount)], key))
#             elif isinstance(amount, pd.Series):
#                 # If amount is a series, apply element-wise slicing
#                 return [str(k)[:int(a)] for k, a in zip(key, amount)]
#         elif isinstance(key, pd.Series):
#             # If key is a pandas Series
#             if isinstance(amount, (int, float)):
#                 return key.map(lambda x: str(x)[:int(amount)])
#             elif isinstance(amount, pd.Series):
#                 # If both key and amount are Series, apply element-wise slicing
#                 return key.combine(amount, lambda k, a: str(k)[:int(a)])
#         else:
#             return key
#     except Exception as ex:
#         raise Exception(str(ex))

def RIGHT(key, amount):
    try:
        if isinstance(key, pd.Series) and isinstance(amount, pd.Series):
            return pd.Series([
                RIGHT(k, a) if not pd.isna(k) and not pd.isna(a) else np.nan
                for k, a in zip(key, amount)
            ])
        elif isinstance(key, str):
            return key[-amount:]
        # elif type(key) == list:
        #     return list(map(lambda x: str(x)[-amount:]))
        elif isinstance(key, (int, float)):
            return str(int(key))[-amount:] if float(key).is_integer() else str(key)[-amount:]
        elif isinstance(key, pd.Series):
            return key.apply(lambda x: RIGHT(x, amount) if not pd.isna(x) else np.nan)
        else:
            return key
    except Exception as ex:
        raise Exception(str(ex))


# def MID(key, offset, amount):
#     try:
#         if type(key) == str:
#             return key[offset -1 :offset + amount -1]
#         elif type(key) == list:
#             return list(map(lambda x: str(x)[offset -1 :offset + amount -1]))
#         elif isinstance(key, pd.Series):
#             return key.map(lambda x: str(x)[offset -1 :offset + amount -1] if not pd.isna(x) else np.nan)
#         else:
#             return key
#     except Exception as ex:
#         raise Exception(str(ex))

def MID(key, offset, amount):
    try:
        if isinstance(key, str):
            # Single string case
            return key[offset - 1:offset + amount - 1]
        elif isinstance(key, list):
            # List case - assuming offset and amount are constants
            return list(map(lambda x: str(x)[offset - 1:offset + amount - 1] if pd.notna(x) else np.nan, key))
        elif isinstance(key, pd.Series):
            # Series case
            if isinstance(offset, pd.Series) and isinstance(amount, pd.Series):
                # Ensure key, offset, and amount all have the same length
                if len(key) != len(offset) or len(key) != len(amount):
                    raise ValueError("key, offset, and amount must all have the same length.")

                # Use zip to iterate over key, offset, and amount simultaneously
                return pd.Series([str(k)[o - 1: o + a - 1] if pd.notna(k) else np.nan
                                  for k, o, a in zip(key, offset, amount)])
            elif isinstance(offset, int) and isinstance(amount, pd.Series):
                # Constant offset, Series amount
                return pd.Series([str(k)[offset - 1: offset + a - 1] if pd.notna(k) else np.nan
                                  for k, a in zip(key, amount)])
            elif isinstance(offset, pd.Series) and isinstance(amount, int):
                # Series offset, constant amount
                return pd.Series([str(k)[o - 1: o + amount - 1] if pd.notna(k) else np.nan
                                  for k, o in zip(key, offset)])
            else:
                # Both offset and amount are constants
                return key.map(lambda x: str(x)[offset - 1:offset + amount - 1] if pd.notna(x) else np.nan)
        else:
            return key
    except Exception as ex:
        raise Exception(str(ex))


def TRIM(key):
    try:
        if type(key) == str:
            return key.strip()
        elif type(key) == list:
            return list(map(lambda x: str(x).strip()))
        elif isinstance(key, pd.Series):
            # return key.map(lambda x: str(x).strip())
            return key.map(lambda x: str(x).strip() if not pd.isna(x) else x)
        else:
            return key
    except Exception as ex:
        raise Exception(str(ex))


def LTRIM(key):
    try:
        if type(key) == str:
            return key.lstrip()
        elif type(key) == list:
            return list(map(lambda x: str(x).lstrip()))
        elif isinstance(key, pd.Series):
            return key.map(lambda x: str(x).lstrip() if not pd.isna(x) else x)
        else:
            return key
    except Exception as ex:
        raise Exception(str(ex))

def RTRIM(key):
    try:
        if type(key) == str:
            return key.rstrip()
        elif type(key) == list:
            return list(map(lambda x: str(x).rstrip()))
        elif isinstance(key, pd.Series):
            # return key.map(lambda x: str(x).rstrip())
            return key.map(lambda x: str(x).rstrip() if not pd.isna(x) else x)
        else:
            return key
    except Exception as ex:
        raise Exception(str(ex))


def CLEAN(key):
    try:
        if type(key) == str:
            return remove_control_chars(key)
        elif type(key) == list:
            return list(map(lambda x: remove_control_chars(x)))
        elif isinstance(key, pd.Series):
            return key.map(lambda x: remove_control_chars(str(x)) if pd.isna(x) else x)
        else:
            return key
    except Exception as ex:
        raise Exception(str(ex))

def SUBSTITUTE(key, old, new, count=0):
    def safe_replace(x):
        if pd.isna(x) or x is None:
            return x  # Keep None or NaN as is
        x_str = str(x)  # Convert numbers, dates, etc. to string
        return x_str.replace(old, new) if count == 0 else x_str.replace(old, new, count)
    try:
        if isinstance(key, str):
            return safe_replace(key)

        elif isinstance(key, list):
            return [safe_replace(x) for x in key]

        elif isinstance(key, pd.Series):
            return key.map(safe_replace)
        else:
            return safe_replace(key)  # fallback for scalar values
    except Exception as ex:
        raise Exception(ex)

def FIND(find_text,key, start_num=0):
    try:
        if type(key) == str:
            return key.find(find_text, start_num) + 1
        elif isinstance(key, (int, float)):
            return str(key).find(find_text, start_num) + 1
        elif type(key) == list:
            return key.astype(str).map(map(lambda x: x.find(str(find_text), start_num) + 1))
        elif isinstance(key, pd.Series):
            return key.astype(str).map(lambda x: x.find(str(find_text), start_num) + 1)
        else:
            return key
    except Exception as ex:
        raise Exception(str(ex))


def REPT(key, repeats):
    try:
        if type(key) == str:
            return key * repeats
        elif type(key) == list:
            return list(map(lambda x: str(x) * repeats))
        elif isinstance(key, pd.Series):
            return key.map(lambda x: str(x) * repeats)
        else:
            return key
    except Exception as ex:
        raise Exception(str(ex))


def remove_control_chars(a_string):
    try:
        filtered_characters = list(s for s in a_string if s.isprintable())
        filtered_string = ''.join(filtered_characters)
        return filtered_string
    except Exception as ex:
        raise Exception(str(ex))


def SUM(*args):
    try:
        arg_len = len(args)
        if arg_len == 1:
            arg = args[0]
            final_sum = 0
            if isinstance(arg, (int, float, np.number)):
                final_sum = arg
            elif type(arg) == list:
                numeric_values = [x for x in arg if isinstance(x, (int, float))]
                final_sum = builtins.sum(numeric_values)
            elif isinstance(arg, pd.Series):
                numeric_series = pd.to_numeric(arg, errors='coerce')
                # Filter out non-numeric values
                # filtered_series = numeric_series.dropna()
                filtered_series = numeric_series.fillna(0)
                final_sum = builtins.sum(filtered_series.tolist())
            return final_sum
        else:
            final_sum = 0
            for arg in args:
                # if isinstance(arg, int) or isinstance(arg, float):
                if isinstance(arg, (int, float, np.number)):
                    final_sum += arg
                elif isinstance(arg, pd.Series):
                    numeric_series = pd.to_numeric(arg, errors='coerce')
                    # Filter out non-numeric values
                    # filtered_series = numeric_series.dropna()
                    filtered_series = numeric_series.fillna(0)
                    final_sum += filtered_series
            return final_sum
    except Exception as ex:
        raise Exception(str(ex))


def COUNT(*args):
    try:
        final_list =[]
        for arg in args:
            # if isinstance(arg, int) or isinstance(arg, float):
            if isinstance(arg, (int, float, np.number)):
                final_list.append(arg)
            elif isinstance(arg, str):
                if arg.isdigit():
                    final_list.append(arg)
            elif type(arg) == list:
                numeric_values = [x for x in arg if isinstance(x, (int, float))]
                final_list.extend(numeric_values)
            elif isinstance(arg, pd.Series):
                numeric_series = pd.to_numeric(arg, errors='coerce')
                # Filter out non-numeric values
                filtered_series = numeric_series.dropna()
                final_list.extend(filtered_series.tolist())
        return len(final_list)
    except Exception as ex:
        raise Exception(str(ex))


# def AVERAGE(*args):
#     try:
#         arg_len = len(args)
#         if arg_len == 1:
#             final_avg = []
#             arg = args[0]
#             # if isinstance(arg, int) or isinstance(arg, float):
#             if isinstance(arg, (int, float, np.number)):
#                 final_avg = arg
#             elif type(arg) == list:
#                 numeric_values = [x for x in arg if isinstance(x, (int, float))]
#                 final_avg = statistics.mean(numeric_values)
#             elif isinstance(arg, pd.Series):
#                 numeric_series = pd.to_numeric(arg, errors='coerce')
#                 # Filter out non-numeric values
#                 filtered_series = numeric_series.dropna()
#                 final_avg = statistics.mean(filtered_series)
#             return final_avg
#         else:
#             final_list = []
#             for arg in args:
#                 if isinstance(arg, (int, float, np.number)):
#                     final_list.append(arg)
#                 elif type(arg) == list:
#                     numeric_values = [x for x in arg if isinstance(x, (int, float))]
#                     final_list.extend(numeric_values)
#                 elif isinstance(arg, pd.Series):
#                     numeric_series = pd.to_numeric(arg, errors='coerce')
#                     # Filter out non-numeric values
#                     filtered_series = numeric_series.dropna()
#                     final_list.extend(filtered_series.tolist())
#             if len(final_list) > 0:
#                 return statistics.mean(final_list)
#     except Exception as ex:
#         raise Exception(str(ex))

def AVERAGE(*args):
    try:
        arg_len = len(args)
        if arg_len == 1:
            final_avg = []
            arg = args[0]
            # if isinstance(arg, int) or isinstance(arg, float):
            if isinstance(arg, (int, float, np.number)):
                final_avg = arg
            elif type(arg) == list:
                numeric_values = [x for x in arg if isinstance(x, (int, float))]
                final_avg = statistics.mean(numeric_values)
            elif isinstance(arg, pd.Series):
                numeric_series = pd.to_numeric(arg, errors='coerce')
                # Filter out non-numeric values
                filtered_series = numeric_series.dropna()
                final_avg = statistics.mean(filtered_series)
            return final_avg
        else:
            def avg_cell(cell_values):
                numeric_values = [x for x in cell_values if isinstance(x, (int, float))]
                return sum(numeric_values) / len(numeric_values) if numeric_values else np.nan

            series_list = []
            non_series_items = []

            # Separate series and non-series items
            for arg in args:
                if isinstance(arg, pd.Series):
                    series_list.append(arg)
                elif isinstance(arg, list):
                    if len(arg) == 1 and isinstance(arg[0], pd.Series):
                        series_list.append(arg[0])
                    else:
                        non_series_items.extend(arg)
                else:
                    non_series_items.append(arg)

            # Handle scenarios with no series
            if not series_list:
                if non_series_items:
                    return sum(non_series_items) / len(non_series_items)
                else:
                    return np.nan

            # Handle scenarios with single series
            elif len(series_list) == 1:
                series = series_list[0]
                result_series = pd.Series(index=series.index)
                for index, value in series.items():
                    cell_values = [value] + non_series_items
                    result_series[index] = avg_cell(cell_values)
                return result_series

            # Handle scenarios with multiple series
            else:
                result_series = pd.concat(series_list, axis=1)
                result_series = result_series.apply(lambda x: avg_cell(list(x) + non_series_items), axis=1)
                return result_series

    except Exception as ex:
        raise Exception(str(ex))


def ABS(*args):
    try:
        key = args[0]
        # if type(key) == float or type(key) == int:
        if isinstance(key, (int, float, np.number)):
            if key < 0:
                return np.abs(key)  # Use np.abs for negative numbers
            else:
                return key
        elif type(key) == list:
            return list(map(lambda x: np.abs(x) if isinstance(x, (int, float)) and x < 0 else x, key))
        elif isinstance(key, pd.Series) and key.size > 0:
            return key.apply(lambda x: np.abs(x) if isinstance(x, (int, float)) and x < 0 else x)
        elif type(key) == str:
            raise Exception("invalid formula")
    except Exception as ex:
        raise Exception(str(ex))


def FACT(key):
    try:
        if type(key) == float or type(key) == int:
            return np.math.factorial(int(key))
        elif type(key) == list:
            return list(map(lambda x: np.math.factorial(int(x)) if isinstance(x, (int, float)) else x, key))
        elif isinstance(key,pd.Series) and key.size > 0:
            return key.apply(lambda x: np.math.factorial(int(x)) if isinstance(x, (int, float)) else x)
        elif type(key) == str:
            return key
        else:
            raise Exception("invalid formula")
    except Exception as ex:
        raise Exception(str(ex))


def FLOOR(key, significance):
    try:
        if type(key) == float or type(key) == int:
            return np.floor_divide(key,significance)*significance
        elif type(key) == list:
            return list(map(lambda x: np.floor_divide(x,significance)*significance if isinstance(x, (int, float)) and not math.isnan(x) else x, key))
        elif isinstance(key,pd.Series) and key.size > 0:
            return key.apply(lambda x: np.floor_divide(x,significance)*significance if isinstance(x, (int, float)) and not math.isnan(x) else x)
        elif type(key) == str :
            num = float(key)
            return np.floor_divide(num,significance)*significance
    except Exception as ex:
        raise Exception(str(ex))


def CEILING(key, significance):
    try:
        if type(key) == float or type(key) == int:
            return math.ceil(key / significance) * significance
        elif type(key) == list:
            return list(map(lambda x:  math.ceil(x / significance) * significance if isinstance(x, (int, float)) and not math.isnan(x) else x, key))
        elif isinstance(key,pd.Series) and key.size > 0:
            return key.apply(lambda x:  math.ceil(x / significance) * significance if isinstance(x, (int, float)) and not math.isnan(x) else x)
        elif type(key) == str:
            num = float(key)
            return np.floor_divide(num,significance)*significance
    except Exception as ex:
        raise Exception(str(ex))


def FACTDOUBLE(key):
    try:
        if type(key) == float or type(key) == int:
            return doublefactorial(int(key))
        elif type(key) == list:
            return list(map(lambda x: doublefactorial(int(x)) if isinstance(x, (int, float)) else x, key))
        elif isinstance(key,pd.Series) and key.size > 0:
            return key.apply(lambda x: doublefactorial(int(x)) if isinstance(x, (int, float)) and not math.isnan(x) else x)
        elif type(key) == str:
            return key
        else:
            raise Exception("invalid formula")
    except Exception as ex:
        raise Exception(str(ex))


def doublefactorial(n):
    try:
        if n == 0 or n == 1:
            return 1
        else:
            return n * doublefactorial(n - 2)
    except Exception as ex:
        raise Exception(str(ex))


def POWER(key, number):
    try:
        if type(key) == float or type(key) == int:
            return np.math.pow(key, number)
        elif type(key) == list:
            return list(map(lambda x: np.math.pow(x, number), key))
        elif isinstance(key,pd.Series) and key.size > 0:
            return key.map(lambda x: np.math.pow(x, number))
        else:
            return key
    except Exception as ex:
        raise Exception(str(ex))


def EXP(key):
    try:
        if (type(key) == float or type(key) == int) and not math.isnan(key):
            return math.exp(key)
        elif type(key) == list:
            return list(map(lambda x: math.exp(x) if (
                    (type(x) == float or type(x) == int) and not math.isnan(x)) else x))
        elif isinstance(key,pd.Series):
            result = key.map(
                lambda x: math.exp(x) if ((type(x) == float or type(x) == int) and not math.isnan(x)) else x)
            return result
        else:
            return key
    except Exception as ex:
        raise Exception(str(ex))


def SQRT(key):
    def safe_sqrt(x):
        try:
            if isinstance(x, (int, float)) and not math.isnan(x) and x >= 0:
                return math.sqrt(x)
        except:
            pass
        return np.nan  # Return np.nan for invalid inputs

    try:
        if isinstance(key, (int, float)):
            return safe_sqrt(key)
        elif isinstance(key, list):
            return [safe_sqrt(x) for x in key]
        elif isinstance(key, pd.Series):
            return key.map(safe_sqrt)
        else:
            return np.nan  # Unsupported input types also return np.nan
    except Exception as ex:
        raise Exception(ex)

def ROUNDVAL(key, digits=0):
    try:
        if (type(key) == float or type(key) == int) and not math.isnan(key):
            return round(key, digits)
        elif type(key) == list:
            return list(map(lambda x: round(x, digits) if (
                    (type(x) == float or type(x) == int) and not math.isnan(x)) else x))
        elif isinstance(key,pd.Series) and key.size > 0:
            result = key.map(
                lambda x: round(x, digits) if ((type(x) == float or type(x) == int) and not math.isnan(x)) else x)
            return result
        else:
            return key
    except Exception as ex:
        raise Exception(str(ex))


# date functions
# def DATE(year, month, day):
#     try:
#         date_obj = datetime.datetime(year, month, day)
#         return date_obj.strftime("%m-%d-%Y")
#     except Exception as ex:
#         raise Exception(str(ex))

def DATE(year, month, day):
    try:
        # Check if inputs are all scalar values
        if isinstance(year, (int, float)) and isinstance(month, (int, float)) and isinstance(day, (int, float)):
            date_obj = datetime.datetime(int(year), int(month), int(day))
            # return date_obj.strftime("%m-%d-%Y")
            return date_obj
        else:
            # Create a DataFrame from the inputs to handle mixed types
            df = pd.DataFrame({'year': year, 'month': month, 'day': day})
            # Convert to datetime
            date_obj = pd.to_datetime(df)
            # Format the date as needed
            formatted_date = date_obj.dt.strftime("%m-%d-%Y")
            # return formatted_date
            return date_obj
    except Exception as ex:
        raise Exception(str(ex))


def TODAY(user_timezone="UTC"):
    try:
        if user_timezone not in available_timezones():
            user_timezone = "UTC"

        now = datetime.datetime.now(ZoneInfo(user_timezone)).date()  # returns date
        return pd.to_datetime(now)  # ensures type is Timestamp (datetime64[ns] in DataFrame)
    except Exception as ex:
        raise Exception(str(ex))

def NOW(user_timezone="UTC"):
    try:
        if user_timezone not in available_timezones():
            user_timezone = "UTC"
        result = datetime.datetime.now(ZoneInfo(user_timezone))
        formatted_result = result.strftime("%Y-%m-%d %H:%M")
        return formatted_result
    except Exception as ex:
        raise Exception(ex)

def DATEVALUE(datestring):
    def process_date(ds):
        try:
            # Excel base date
            excel_base_date = datetime.datetime(1899, 12, 30)

            # Case 1: Excel serial number input (e.g., "145")
            if isinstance(ds, (int, float)) and ds < 60000:  # avoid confusion with full years
                return int(ds)

            if isinstance(ds, str) and ds.strip().isdigit():
                return int(ds.strip())

            # Case 2: Already a Timestamp
            if isinstance(ds, pd.Timestamp):
                return (ds - pd.Timestamp(excel_base_date)).days

            # Case 3: String like "12-Dec"
            if isinstance(ds, str):
                ds = ds.strip()

                # Match "12-Dec"
                if re.match(r"^\d{1,2}-[A-Za-z]{3}$", ds):
                    current_year = datetime.datetime.now().year
                    date_obj = datetime.datetime.strptime(f"{ds}-{current_year}", "%d-%b-%Y")
                    return (date_obj - excel_base_date).days

                # Match "YYYY/MM" or "YYYY-MM"
                if re.match(r"^\d{4}[/-]\d{1,2}$", ds):
                    date_obj = pd.to_datetime(ds + "-01")  # assume first day of month
                    return (date_obj - pd.Timestamp(excel_base_date)).days

                # Match full date formats or try parsing
                try:
                    ts = pd.to_datetime(ds, dayfirst=False, errors='raise')  # fallback
                    return (ts - pd.Timestamp(excel_base_date)).days
                except:
                    raise Exception(f"Unrecognized date format: {ds}")

            # Fallback to try parsing anything else
            ts = pd.to_datetime(ds)
            return (ts - pd.Timestamp(excel_base_date)).days

        except Exception as ex:
            return f"Error: {ds} - {str(ex)}"

    # Apply function to Series or single value
    if isinstance(datestring, pd.Series):
        return datestring.apply(process_date)
    else:
        return process_date(datestring)

def DATEDIFF(date1, date2, default=None):
    try:
        #Convert input to pandas datetime if they are not already
        try:
            date1 = pd.to_datetime(date1, format='%x')
        except ValueError:
            date1 = pd.to_datetime(date1)

        try:
            date2 = pd.to_datetime(date2, format='%x')
        except ValueError:
            date2 = pd.to_datetime(date2)

        if isinstance(date1, pd.Series) and isinstance(date2, pd.Series):
            valid_dates = date1.notnull() & date2.notnull()
        elif isinstance(date1, str) and isinstance(date2, str):
            valid_dates = pd.Series(True)
        else:
            val1 = True if isinstance(date1, pd.Timestamp) else date1.notnull()
            val2 = True if isinstance(date2, pd.Timestamp) else date2.notnull()
            valid_dates = val1 & val2
        # # Check for validity of dates
        # valid_dates = date1.notnull() & date2.notnull()
        diff_days = pd.Series(default, index=date1.index)
        # Calculate the difference in days for valid dates
        if isinstance(date1, pd.Series) and isinstance(date2, pd.Series):
            diff_days[valid_dates] = (date1[valid_dates] - date2[valid_dates]).dt.days
        elif isinstance(date1, pd.Series) and isinstance(date2,pd.Timestamp):
            diff_days[valid_dates] = (date1[valid_dates] - date2).dt.days
        elif isinstance(date1, pd.Timestamp) and isinstance(date2, pd.Timestamp):
            diff_days[valid_dates] = (date1 - date2).dt.days
        elif isinstance(date1, pd.Timestamp) and isinstance(date2, pd.Series):
            diff_days[valid_dates] = (date1 - date2[valid_dates]).dt.days

        return diff_days
    except Exception as e:
        print("Error occurred:", e)
        return pd.Series(default, index=date1.index)

def excel_serial_to_date(serial):
    """
    Convert Excel serial number to a Python datetime.date object.
    Handles Excel's 1900 leap year bug.
    """
    if serial < 1:
        raise ValueError("Invalid Excel date serial number")

    base_date = datetime.datetime(1899, 12, 31)
    # Adjust for Excel's leap year bug: subtract 1 extra day if serial >= 60
    delta = datetime.timedelta(days=int(serial))
    if serial >= 60:
        delta -= datetime.timedelta(days=1)
    return base_date + delta

def DAY(val):
    def get_day(x):
        try:
            if pd.isna(x) or (isinstance(x, str) and x.strip() == ""):
                return 0
            if isinstance(x, str) and x.strip().isdigit():
                x = int(x.strip())
            if isinstance(x, (int, float)):
                return excel_serial_to_date(x).day
            if isinstance(x, datetime.datetime):
                return x.day
            if isinstance(x, str):
                x = x.strip()
                if re.match(r"^\d{4}[/-]\d{1,2}$", x):
                    return 0  # no day info
                return parse(x).day
            return 0
        except:
            return 0
    return val.apply(get_day) if isinstance(val, pd.Series) else get_day(val)

def MONTH(val):
    def get_month(x):
        try:
            if pd.isna(x) or (isinstance(x, str) and x.strip() == ""):
                return 0
            if isinstance(x, str) and x.strip().isdigit():
                x = int(x.strip())
            if isinstance(x, (int, float)):
                return excel_serial_to_date(x).month
            if isinstance(x, datetime.datetime):
                return x.month
            if isinstance(x, str):
                x = x.strip()
                if re.match(r"^\d{4}[/-]\d{1,2}$", x):
                    return pd.to_datetime(x + "-01").month
                return parse(x).month
            return 0
        except:
            return 0
    return val.apply(get_month) if isinstance(val, pd.Series) else get_month(val)

def YEAR(val):
    def get_year(x):
        try:
            if pd.isna(x) or (isinstance(x, str) and x.strip() == ""):
                return 0
            if isinstance(x, str) and x.strip().isdigit():
                x = int(x.strip())
            if isinstance(x, (int, float)):
                return excel_serial_to_date(x).year
            if isinstance(x, datetime.datetime):
                return x.year
            if isinstance(x, str):
                x = x.strip()
                if re.match(r"^\d{4}[/-]\d{1,2}$", x):
                    return pd.to_datetime(x + "-01").year
                return parse(x).year
            return 0
        except:
            return 0
    return val.apply(get_year) if isinstance(val, pd.Series) else get_year(val)

def parse_to_date_excel_style(val):
    if pd.isna(val) or (isinstance(val, str) and val.strip() == ""):
        return excel_serial_to_datetime(0)  # Treat blank as serial 0

    if isinstance(val, (int, float)):
        return excel_serial_to_datetime(val)

    if isinstance(val,datetime.datetime):
        return val

    if isinstance(val, str):
        val = val.strip()
        # Numeric strings like "145" should be treated as serials
        if val.replace('.', '', 1).isdigit():
            return excel_serial_to_datetime(float(val))
        try:
            return parse(val, dayfirst=False)
        except:
            return np.nan
    return np.nan

def WEEKDAY(value, return_type=1):
    if return_type not in list(range(1, 4)) + list(range(11, 18)):
        raise ValueError("Invalid return_type. Allowed: 1–3 or 11–17")

    def compute(val):
        dt = parse_to_date_excel_style(val)
        if pd.isna(dt):
            return np.nan

        weekday = dt.weekday()

        if return_type == 1:
            return (weekday + 1) % 7 + 1
        elif return_type == 2:
            return weekday % 7 + 1
        elif return_type == 3:
            return weekday
        elif 11 <= return_type <= 17:
            shift = return_type - 11
            return (weekday - shift) % 7 + 1
        return np.nan

    if isinstance(value, pd.Series):
        return value.apply(compute)
    elif isinstance(value, list):
        return [compute(v) for v in value]
    else:
        return compute(value)

def excel_serial_to_datetime(serial):
    base_date =datetime.datetime(1899, 12, 30)  # Excel base date
    if serial >= 60:
        serial -= 1  # Excel leap year bug adjustment
    days = int(serial)
    fractional = serial - days
    date_part = base_date + timedelta(days=days)
    seconds_in_day = 86400
    time_part = timedelta(seconds=round(fractional * seconds_in_day))
    dt = date_part + time_part
    return dt

def get_hours(val):
    try:
        # Handle None, NaN, empty strings
        if val is None:
            return 0
        if isinstance(val, float) and np.isnan(val):
            return 0
        if isinstance(val, str) and val.strip() == "":
            return 0
        if pd.isna(val):
            return 0

        # If val is a datetime or Timestamp
        if isinstance(val, datetime.datetime):
            return val.hour

        val_str = str(val).strip()

        # Handle "24:00" as 0 hour (midnight)
        if val_str == "24:00":
            return 0

        # If string contains colon, parse as time/datetime string
        if ":" in val_str:
            dt = parser.parse(val_str)
            # Excel treats "24:00" as midnight (0 hour), already handled above
            return dt.hour

        # Try convert string to float → Excel serial
        try:
            val_num = float(val_str)
            dt = excel_serial_to_datetime(val_num)
            return dt.hour
        except ValueError:
            pass

        # Fallback: parse string as date/time
        dt = parser.parse(val_str)
        return dt.hour

    except Exception as ex:
        print(ex)
        return 0

def HOUR(val):
    if isinstance(val, pd.Series):
        return val.apply(get_hours)
    else:
        return get_hours(val)

def get_minutes(val):
    try:
        if val is None or (isinstance(val, float) and np.isnan(val)) or pd.isna(val):
            return 0
        if isinstance(val, str) and val.strip() == "":
            return 0

        if isinstance(val, datetime.datetime):
            return val.minute

        val_str = str(val).strip()

        if val_str == "24:00":
            return 0

        if ":" in val_str:
            dt = parser.parse(val_str)
            return dt.minute

        try:
            val_num = float(val_str)
            dt = excel_serial_to_datetime(val_num)
            return dt.minute
        except ValueError:
            pass

        dt = parser.parse(val_str)
        return dt.minute

    except Exception as ex:
        return 0

def MINUTE(val):
    try:
        if isinstance(val, pd.Series):
            return val.apply(get_minutes)
        elif isinstance(val, (list, np.ndarray)):
            return [get_minutes(v) for v in val]
        return get_minutes(val)
    except Exception as ex:
        raise Exception(ex)

def convert_date_to_excel_ordinal(day, month, year):
    # Specifying offset value i.e.,
    # the date value for the date of 1900-01-00
    try:
        offset = 693594
        current = datetime.datetime(year, month, day)
        n = current.toordinal()
        return n - offset
    except Exception as ex:
        raise Exception(str(ex))


def excel_date(date1):
    # Initializing a reference date
    # Note that here date is not 31st Dec but 30th!
    try:
        temp = datetime.datetime(1899, 12, 30)
        delta = date1 - temp
        return float(delta.days) + (float(delta.seconds) / 86400)
    except Exception as ex:
        raise Exception(str(ex))


# def RANK(number, ref, order=0):
#     try:
#         # Find the rank in the series
#         if isinstance(ref, pd.Series):
#             if order == 0:
#                 rank_series = ref.rank(ascending=False).get(ref == number).values[0]
#                 return rank_series
#             else:
#                 rank_series = ref.rank(ascending=True).get(ref == number).values[0]
#                 return rank_series
#         elif isinstance(ref, list):
#             ascending = False
#             if order == 0:
#                 sorted_lst = sorted(ref) if ascending else sorted(ref, reverse=True)
#                 rank = sorted_lst.index(number) + 1
#                 return rank
#             else:
#                 ascending = True
#                 sorted_lst = sorted(ref) if ascending else sorted(ref, reverse=True)
#                 try:
#                     rank = sorted_lst.index(number) + 1
#                     return rank
#                 except ValueError:
#                     return None
#         else:
#             raise Exception("invalid input.")
#     except Exception as ex:
#         raise Exception(str(ex))


def RANK(number, ref, order=0):
    try:
        # Check if 'number' or 'ref' contains any NaN or is empty
        if number is None or ref is None or ref.empty:
            return "#N/A"

        # If 'ref' contains the same value for all elements, assign rank 1 (or length of ref if ascending)
        if ref.nunique() == 1:
            if isinstance(number, pd.Series):
                return number.apply(lambda x: 1 if ref.iloc[0] == x else "#N/A")
            else:
                return 1 if ref.iloc[0] == number else "#N/A"

        # If 'number' is a Series, filter out NaN values
        if isinstance(number, pd.Series):
            number = number.dropna()

            if number.empty:
                return "#N/A"

            # Using 'min' method to ensure same ranks for duplicates without decimals
            if order == 0:
                return number.apply(
                    lambda x: ref.rank(ascending=False, method='min').loc[ref == x].values[0] if not ref[
                        ref == x].empty else "#N/A")
            else:
                return number.apply(
                    lambda x: ref.rank(ascending=True, method='min').loc[ref == x].values[0] if not ref[
                        ref == x].empty else "#N/A")
        else:
            # If 'number' is a single value, ensure it's not NaN
            if pd.isna(number):
                return "#N/A"

            # Using 'min' method for single value as well
            if order == 0:
                return ref.rank(ascending=False, method='min').loc[ref == number].values[0] if not ref[
                    ref == number].empty else "#N/A"
            else:
                return ref.rank(ascending=True, method='min').loc[ref == number].values[0] if not ref[
                    ref == number].empty else "#N/A"
    except IndexError:
        # Handle cases where the number doesn't exist in the reference
        return "#N\A"
    except Exception as ex:
        raise Exception(str(ex))


def RANKEQ(number, ref: pd.Series, order: int = 0):

    if not isinstance(ref, pd.Series):
        raise ValueError("ref must be a column")

    numeric_ref = pd.to_numeric(ref, errors='coerce')

    ranks = numeric_ref.rank(method="min", ascending=(order == 1))

    def get_rank(n):
        n = pd.to_numeric(n, errors='coerce')
        if pd.isna(n) or n not in numeric_ref.values:
            return "#N/A"  # Return None if the number isn't found in reference

        # Get rank based on first or last occurrence
        idx = 0 if order == 1 else -1  # First match for ascending, last match for descending
        return int(ranks[numeric_ref == n].iloc[idx])

    if isinstance(number, pd.Series):
        return number.apply(get_rank)

    # Single value case
    return get_rank(number)


def MATCH(lookup_value, data, match_type=0):
    try:
        if match_type == 0:
            # Exact match
            position_exact = excel_match(lookup_value, data, match_type)
            return position_exact  # Output: 3
        elif match_type == -1:
            # Next smallest value match
            position_smallest = excel_match(lookup_value, data, match_type)
            return position_smallest
        elif match_type == 1:
            # Next largest value match
            position_largest = excel_match(lookup_value, data, match_type)
            return position_largest
    except Exception as ex:
        raise Exception(str(ex))


# old
# def excel_match(lookup_value, lookup_series, match_type):
#     try:
#         if match_type not in (-1, 0, 1):
#             raise ValueError("Invalid match_type. Choose -1, 0, or 1.")
#
#         if len(lookup_series) == 0:
#             raise ValueError("The lookup array is empty.")
#
#         sorted_indices = np.argsort(lookup_series.values)
#
#         if isinstance(lookup_value, pd.Series):
#             match_indices = []
#             for value in lookup_value:
#                 if match_type == 0:
#                     if value in lookup_series.values:
#                         index = np.where(lookup_series.values == value)[0][0]
#                         match_indices.append(index + 1)
#                     else:
#                         match_indices.append('#N\A')
#                         # raise ValueError("The lookup value was not found in the array.")
#                 elif match_type == -1:
#                     sorted_values = lookup_series.values[sorted_indices]
#                     index = np.where(sorted_values <= value)[0]
#                     if not len(index) == 0:
#                         match_indices.append(index[-1] + 1)
#                     else:
#                         match_indices.append('#N\A')
#                 else:  # match_type == 1
#                     sorted_values = lookup_series.values[sorted_indices]
#                     index = np.where(sorted_values >= value)[0]
#                     if not len(index) == 0:
#                         match_indices.append(index[0] + 1)
#                     else:
#                         match_indices.append('#N\A')
#             return match_indices
#         else:
#             if match_type == 0:
#                 if lookup_value in lookup_series.values:
#                     index = np.where(lookup_series.values == lookup_value)[0][0]
#                     return sorted_indices.tolist().index(index) + 1
#                 else:
#                     return '#N\A'
#             elif match_type == -1:
#                 sorted_values = lookup_series.values[sorted_indices]
#                 index = np.where(sorted_values <= lookup_value)[0]
#                 if not len(index) == 0:
#                     return index[-1] + 1
#                 else:
#                     return '#N\A'
#             else:  # match_type == 1
#                 sorted_values = lookup_series.values[sorted_indices]
#                 index = np.where(sorted_values >= lookup_value)[0]
#                 if not len(index) == 0:
#                     return index[0] + 1
#                 else:
#                     return '#N\A'
#     except Exception as ex:
#         raise Exception(str(ex))

def excel_match(lookup_value, lookup_series, match_type):
    try:
        if match_type not in (-1, 0, 1):
            raise ValueError("Invalid match_type. Choose -1, 0, or 1.")

        if len(lookup_series) == 0:
            raise ValueError("The lookup array is empty.")

        if isinstance(lookup_value, pd.Series):
            match_indices = []
            for value in lookup_value:
                if match_type == 0:
                    if pd.isna(value):
                        # Find the index of the first NaN value
                        nan_indices = np.where(pd.isna(lookup_series.values))[0]
                        if len(nan_indices) > 0:
                            index = nan_indices[0]
                            match_indices.append(index + 1)
                        else:
                            match_indices.append('#N\A')
                    else:
                        if value in lookup_series.values:
                            index = np.where(lookup_series.values == value)[0][0]
                            match_indices.append(index + 1)
                        else:
                            match_indices.append('#N\A')
                elif match_type == -1:
                    sorted_values = lookup_series.sort_values().values
                    index = np.where(sorted_values <= value)[0]
                    if not len(index) == 0:
                        match_indices.append(index[-1] + 1)
                    else:
                        match_indices.append('#N\A')
                else:  # match_type == 1
                    sorted_values = lookup_series.sort_values().values
                    index = np.where(sorted_values >= value)[0]
                    if not len(index) == 0:
                        match_indices.append(index[0] + 1)
                    else:
                        match_indices.append('#N\A')
            return match_indices
        else:
            if match_type == 0:
                if lookup_value in lookup_series.values:
                    index = np.where(lookup_series.values == lookup_value)[0][0]
                    return index + 1
                else:
                    return '#N\A'
            elif match_type == -1:
                sorted_values = lookup_series.sort_values().values
                index = np.where(sorted_values <= lookup_value)[0]
                if not len(index) == 0:
                    return index[-1] + 1
                else:
                    return '#N\A'
            else:  # match_type == 1
                sorted_values = lookup_series.sort_values().values
                index = np.where(sorted_values >= lookup_value)[0]
                if not len(index) == 0:
                    return index[0] + 1
                else:
                    return '#N\A'
    except Exception as ex:
        raise Exception(str(ex))

def MEAN(*args):
    try:
        finallist = []
        for arg in args:
            if isinstance(arg, int) or isinstance(arg,float):
                finallist.append(arg)
            elif type(arg) == list:
                finallist.extend(arg)
            elif isinstance(arg, pd.Series):
                lst = arg.apply(lambda x: pd.to_numeric(x, errors='coerce')).isna().tolist()
                if any(lst):
                    raise Exception("invalid input")
                else:
                    finallist.extend(arg.tolist())
        mean = statistics.mean(finallist)
        return mean
    except Exception as ex:
            raise Exception(str(ex))


def MEDIAN(*args):
    try:
        finallist = []
        for arg in args:
            if isinstance(arg, int) or isinstance(arg, float):
                finallist.append(arg)
            elif type(arg) == list:
                finallist.extend(arg)
            elif isinstance(arg, pd.Series):
                lst = arg.apply(lambda x: pd.to_numeric(x, errors='coerce')).isna().tolist()
                if any(lst):
                    raise Exception("invalid input")
                else:
                    finallist.extend(arg.tolist())
        median = statistics.median(finallist)
        return median
    except Exception as ex:
        raise Exception(str(ex))


def MODE(*args):
    try:
        finallist = []
        for arg in args:
            if isinstance(arg, int) or isinstance(arg, float):
                finallist.append(arg)
            elif type(arg) == list:
                finallist.extend(arg)
            elif isinstance(arg, pd.Series):
                lst = arg.apply(lambda x: pd.to_numeric(x, errors='coerce')).isna().tolist()
                if any(lst):
                    raise Exception("invalid input")
                else:
                    finallist.extend(arg.tolist())
        mode = statistics.mode(finallist)
        return mode
    except Exception as ex:
        raise Exception(str(ex))


# def CONCAT(*args):
#     try:
#         finalstring = ""
#         for arg in args:
#             if isinstance(arg, int) or isinstance(arg, float) or isinstance(arg, str):
#                 finalstring = finalstring + str(arg)
#             elif isinstance(arg, pd.Series):
#                 finalstring = finalstring + arg.fillna("").astype(str)
#             else:
#                 return finalstring
#         return finalstring
#     except Exception as ex:
#         raise Exception(str(ex))


def CONCAT(*args):
    try:
        max_length = max([len(arg) for arg in args if isinstance(arg, pd.Series)], default=0)
        result = pd.Series([""] * max_length) if max_length != 0 else ""

        for arg in args:
            if isinstance(arg, pd.Series):
                if pd.api.types.is_datetime64_any_dtype(arg):
                    arg = arg.astype(str).replace('NaT', "")
                extended_series = arg.reindex(range(max_length), fill_value="")
                result += extended_series.apply(lambda x: "" if pd.isna(x) else (
                    str(int(x)) if isinstance(x, float) and x.is_integer() else str(x)))
            else:
                result += str(arg)
        return result
    except Exception as ex:
        raise Exception(str(ex))


# def IFERROR(*args):
#     try:
#         result = eval(args[0])
#         return result
#     except Exception as ex:
#         return args[1]


def STDEV(*args):
    try:
        finallist = []
        for arg in args:
            if isinstance(arg, int) or isinstance(arg, float):
                finallist.append(arg)
            elif type(arg) == list:
                finallist.extend(arg)
            elif isinstance(arg, pd.Series):
                lst = arg.apply(lambda x: pd.to_numeric(x, errors='coerce')).isna().tolist()
                if any(lst):
                    raise Exception("invalid input")
                else:
                    finallist.extend(arg.tolist())
        mean = statistics.mean(finallist)
        result = statistics.stdev(finallist, mean)
        return result
    except Exception as ex:
        raise Exception(str(ex))


def CORREL(*args):
    try:
        if isinstance(args, tuple):
            if isinstance(args[0], list) & isinstance(args[1], list):
                result = get_correl_op(args[0], args[1])
                return result
            elif isinstance(args[0], pd.Series) & isinstance(args[1], pd.Series):
                result = get_correl_op(args[0].values, args[1].values)
                return result
            elif isinstance(args[0], pd.Series) & isinstance(args[1], list):
                result = get_correl_op(args[0].values, args[1])
                return result
            elif isinstance(args[0], list) & isinstance(args[1], pd.Series):
                result = get_correl_op(args[0], args[1].values)
                return result
        else:
            raise Exception("invalid input")
    except Exception as ex:
        raise Exception(str(ex))


def get_correl_op(arg1, arg2):
    if len(arg1) != len(arg2):
        raise Exception("length of both inputs should be same.")
    else:
        mask = ~np.isnan(arg1) & ~np.isnan(arg2)
        result = np.corrcoef(arg1[mask], arg2[mask])[0, 1]
        return result


# def IFERROR(value, value_if_error):
#     try:
#         if isinstance(value, int) or isinstance(value, float):
#             return eval(value)
#         elif type(value) == str:
#             return eval(value)
#         elif type(value) == list:
#             return eval(value)
#         elif isinstance(value, pd.Series):
#             value.apply(lambda x: IFERROR(lambda: x, value_if_error))
#     except Exception:
#         return value_if_error


# def INDEX(array,row_num=None,column_num=None):
#     try:
#         if isinstance(array,pd.DataFrame):
#             if row_num is not None and column_num is not None:
#                 value = array.iloc[row_num,column_num]
#                 return value
#             elif row_num is None:
#                 value = array.iloc[:,column_num-1]
#                 return value
#             elif column_num is None:
#                 value = array.iloc[row_num-1]
#                 return value
#             elif row_num == 0  and column_num == 0:
#                 return array
#
#         if isinstance(array, pd.Series):
#             column_values = array.tolist()
#             if 0 <= row_num <= len(column_values):
#                 return column_values[row_num-1]
#             else:
#                 raise ValueError("Invalid index")
#         elif isinstance(array, list):
#             if 0 <= row_num <= len(array):
#                 return array[row_num-1]
#             else:
#                 raise ValueError("Invalid index")
#         else:
#             raise ValueError("Invalid data")
#     except Exception as ex:
#         raise Exception(str(ex))


def INDEX(array, row_num, column_num=None):
    try:
        # Handling when row_num is a Series
        if isinstance(row_num, pd.Series):
            results = []
            for r in row_num:
                try:
                    r = int(r)
                except ValueError:
                    pass

                if isinstance(r, int) and r > 0:
                    if column_num is not None:
                        if isinstance(column_num, pd.Series):
                            col_results = []
                            for c in column_num:
                                if isinstance(c, str) and c.isdigit():
                                    c = int(c)

                                if isinstance(c, int) and c > 0:
                                    if isinstance(array, pd.DataFrame):
                                        if r <= len(array) and c <= len(array.columns):
                                            col_results.append(array.iloc[r - 1, c - 1])
                                        else:
                                            col_results.append('#N\A')
                                    else:
                                        col_results.append('#N\A')
                                else:
                                    col_results.append('#N\A')
                            results.append(col_results)
                        else:
                            if isinstance(array, pd.DataFrame):
                                if r <= len(array) and column_num <= len(array.columns):
                                    results.append(array.iloc[r - 1, column_num - 1])
                                else:
                                    results.append('#N\A')
                            else:
                                results.append('#N\A')
                    else:
                        if isinstance(array, (pd.Series, pd.DataFrame)):
                            if isinstance(array, pd.DataFrame):
                                array = array.iloc[:, 0]
                            if r <= len(array):
                                results.append(array.iloc[r - 1])
                            else:
                                results.append('#N\A')
                        else:
                            results.append('#N\A')
                else:
                    results.append('#N\A')
            return pd.Series(results)

        # Handling when column_num is a Series
        if isinstance(column_num, pd.Series):
            results = []
            for c in column_num:
                if isinstance(c, str) and c.isdigit():
                    c = int(c)

                if isinstance(c, int) and c > 0:
                    if isinstance(array, pd.DataFrame):
                        if row_num <= len(array) and c <= len(array.columns):
                            results.append(array.iloc[row_num - 1, c - 1])
                        else:
                            results.append('#N\A')
                    else:
                        results.append('#N\A')
                else:
                    results.append('#N\A')
            return pd.Series(results)

        # Handling static values for row_num and column_num
        if column_num is not None:
            if isinstance(column_num, str) and column_num.isdigit():
                column_num = int(column_num)

            if isinstance(row_num, str) and row_num.isdigit():
                row_num = int(row_num)

            if isinstance(array, pd.DataFrame):
                if row_num <= len(array) and column_num <= len(array.columns):
                    return array.iloc[row_num - 1, column_num - 1]
                else:
                    return '#N\A'
            else:
                raise ValueError("For row and column indexing, array must be a DataFrame.")
        else:
            if isinstance(row_num, str) and row_num.isdigit():
                row_num = int(row_num)

            if row_num <= 0:
                return '#N\A'

            if isinstance(array, (pd.Series, pd.DataFrame)):
                if isinstance(array, pd.DataFrame):
                    array = array.iloc[:, 0]
                if row_num <= len(array):
                    return array.iloc[row_num - 1]
                else:
                    return '#N\A'
            else:
                raise ValueError("array must be a Series or DataFrame.")

    except Exception as ex:
        raise Exception(str(ex))


def TEXT(value, format_string):
    try:
        global isToNumeric
        isToNumeric = False

        fraction_formats = {
            "# ?/?": lambda value: get_format_fraction(value)
        }

        # if format_string in number_formats:
        if is_numeric_format(format_string):
            if isinstance(value, (int, float)):
                # python_format = excel_to_python_format(format_string)
                return format_numeric(value, format_string)
                # return get_number_format(value,python_format)
            elif isinstance(value, list):
                if any(is_valid_numbers(value)):
                    formatted_numbers = list(map(lambda x: format_numeric(x, format_string), value))
                    return formatted_numbers
            elif isinstance(value, pd.Series):
                lst = value.apply(lambda x: format_numeric(x, format_string) if not pd.isna(x) else x)
                return lst

        elif format_string in fraction_formats:
            if isinstance(value, (int, float)):
                # formatter = fraction_formats[format_string]
                # result = formatter(value)
                # return result
                function = fraction_formats[format_string]
                result = function(value)
                return result
            elif isinstance(value, list):
                formatted_numbers = list(map(lambda x: fraction_formats[format_string](x), value))
                return formatted_numbers
            elif isinstance(value, pd.Series):
                lst = value.apply(lambda x: fraction_formats[format_string](x) if not pd.isna(x) else x)
                return lst

        elif is_date_format(format_string):
            format_string = convertExcelDateFormatToPython(format_string);
            if isinstance(value, (int, str, datetime.datetime, datetime.time)):
                if is_valid_date(value):
                    return get_date_time_format(value,format_string)
                else:
                    return get_date_time_format(value, format_string)
            elif isinstance(value, list):
                if any(is_valid_date(value)):
                    formatted_dates = list(map(lambda x : get_date_time_format(x,format_string), value))
                    return formatted_dates
            elif isinstance(value, pd.Series):
                lst = value.apply(lambda x: get_date_time_format(x, format_string) if not pd.isna(x) else x)
                return lst
        else:
            raise Exception("Invalid format")

    except Exception as ex:
        raise Exception(str(ex))


def format_numeric(value, format_string):
    try:
        temp = format_string
        sign = ''
        value = convert_to_number_or_keep_as_is(value)
        if value < 0:
            sign = '-'
            value = abs(value)

        if '%' in temp:
            value *= 100
            format_string = format_string.replace('%', '')

        # Safely find the first number placeholder
        hash_index = format_string.find('#') if '#' in format_string else float('inf')
        zero_index = format_string.find('0') if '0' in format_string else float('inf')
        index = min(hash_index, zero_index)

        currency = ""
        if index != float('inf') and index > 0:
            currency = format_string[:index]
            format_string = format_string[index:]

        sections = format_string.split(';')
        section = sections[0] if len(sections) == 1 or value >= 0 else sections[1]

        number_format = section.split('.')
        integer_format = number_format[0]
        decimal_format = number_format[1] if len(number_format) > 1 else ''

        # ROUND value to number of decimal digits BEFORE formatting
        #rounded_value = round(value, len(decimal_format))
        # ROUND value to number of decimal digits BEFORE formatting (Excel-like rounding)
        rounding_precision = '1.' + '0' * len(decimal_format) if decimal_format else '1'
        rounded_value = float(Decimal(str(value)).quantize(Decimal(rounding_precision), rounding=ROUND_HALF_UP))
        integer_part_val = int(rounded_value)
        decimal_part_val = rounded_value - integer_part_val

        # Use the correct rounded values for formatting
        integer_part_formatted = format_integer(integer_part_val, integer_format)
        decimal_part_formatted = format_decimal(decimal_part_val, decimal_format)

        formatted_value = integer_part_formatted + ('.' + decimal_part_formatted if decimal_format else '')

        if '%' in temp:
            formatted_value += '%'

        if currency != "":
            formatted_value = currency + formatted_value

        formatted_value = sign + formatted_value
        return formatted_value
    except Exception as ex:
        return str(ex)

# def format_integer(value, format_string):
#     # Counting number of '#' and '0' in the format string
#
#
#     integerlen = len(str(value))
#     zero_count = 0
#     formatstringlen = len(str(format_string))
#     if integerlen < formatstringlen:
#         tempstring = format_string[:-integerlen]
#         hash_count = format_string.count('#')
#         zero_count = tempstring.count('0')
#     formatted_integer = str(round(value)).zfill(zero_count+integerlen)
#     if ',' in format_string:
#         # Use comma as thousands separator
#         formatted_integer = f"{int(formatted_integer):,.0f}"
#     # Formatting integer part based on format string
#
#
#     return formatted_integer

def format_integer(value, format_string):
    int_value = int(round(value))

    # Detect if we need comma separator
    use_commas = ',' in format_string

    # Clean format string (remove commas to count digit placeholders)
    clean_format = format_string.replace(',', '')

    # Count digit placeholders (mandatory '0's)
    mandatory_digits = clean_format.count('0')

    # Convert value to string and pad with leading zeros
    padded_str = str(int_value).zfill(mandatory_digits)

    if use_commas:
        # Group from right: apply grouping every 3 digits
        reversed_str = padded_str[::-1]
        grouped = ','.join([reversed_str[i:i+3] for i in range(0, len(reversed_str), 3)])
        int_str = grouped[::-1]
    else:
        int_str = padded_str

    return int_str
def format_decimal(value, format_string):
    # Total required digits in decimal
    total_digits = len(format_string)

    # Extract decimal part only and round to required digits
    decimal_part = abs(value - int(value))  # Get decimal part only
    rounded_decimal = round(decimal_part * (10 ** total_digits))

    # Convert to string with leading zeros as necessary
    formatted = f"{rounded_decimal:0{total_digits}d}"

    return formatted

def format_date(value, format_string):
    # Formatting date using strftime() function
    return value.strftime(convert_excel_format_to_strftime(format_string))


def convert_excel_format_to_strftime(format_string):
    # Dictionary mapping Excel format to strftime format
    mapping = {
        'm': '%m', 'mm': '%m', 'mmm': '%b', 'mmmm': '%B',
        'd': '%d', 'dd': '%d', 'ddd': '%a', 'dddd': '%A',
        'yy': '%y', 'yyyy': '%Y',
        'h': '%I', 'hh': '%I', 'H': '%H', 'HH': '%H',
        'm': '%M', 'mm': '%M',
        's': '%S', 'ss': '%S',
        'AM/PM': '%p'
    }

    # Convert Excel format to strftime format
    strftime_format = ''
    for part in re.findall(r'\[[^\]]*\]|[^-MmdHhYySsAaPp]+|[-MmdHhYySsAaPp]+', format_string):
        if part in mapping:
            strftime_format += mapping[part]
        else:
            strftime_format += part

    return strftime_format


# def get_number_format(number, number_format):
#     try:
#         formatted_number = number_format.format(number)
#         return formatted_number
#     except Exception as ex:
#         raise Exception(str(ex))

def is_integer_or_float(value):
    try:
        float_value = float(value)
        int_value = int(float_value)
        if float_value == int_value:
            return "integer"
        else:
            return "float"
    except ValueError:
        return None


def get_number_format(number, number_format):
    numeric_type = is_integer_or_float(number)
    if numeric_type == "integer":
        try:
            formatted_number = number_format.format(int(number))
            return formatted_number
        except Exception as ex:
            raise Exception(str(ex))
    elif numeric_type == "float":
        try:
            formatted_number = number_format.format(float(number))
            return formatted_number
        except Exception as ex:
            raise Exception(str(ex))
    else:
        raise ValueError("The input number is not a valid numeric value.")


def is_valid_numbers(list):
    result = []
    for element in list:
        if isinstance(element, (int, float)):
            result.append(True)
        else:
            result.append(False)
    return result


def get_format_fraction(number):
    if isinstance(number, (int, float)):
        frac = Fraction(number).limit_denominator(9)

        if abs(number) >= 1:
            whole = int(frac)
            remainder = frac - whole
            if remainder.numerator == 0:
                return str(whole)
            return f"{whole} {remainder.numerator}/{remainder.denominator}"
        else:
            return f"{frac.numerator}/{frac.denominator}"
    else:
        return str(number)

def get_date_time_format(value, format_string):
    try:
        if isinstance(value, str):
            date_object = parser.parse(value)
            formatted_date = date_object.strftime(format_string)
        elif isinstance(value, int):
            date_object = datetime.datetime(1900, 1, 1) + datetime.timedelta(days=value - 2)
            formatted_date = date_object.strftime(format_string)
        elif isinstance(value,(str,int,datetime.datetime,datetime.time)):
            formatted_date = value.strftime(format_string)
        return formatted_date
    except Exception as ex:
        return value


def convertExcelDateFormatToPython(format_string):
    try:
        format_mapping = {
            "dddd": "%A",
            "ddd": "%a",
            "mmmm": "%B",
            "mmm": "%b",
            "yyyy": "%Y",
            "yy": "%y",
            "dd": "%d",
            "d": "%#d",  # No leading zero (Windows)
            "mm": "%m",
            "m": "%#m",  # No leading zero (Windows)
            "hh": "%I",  # 12-hour format (leading zero)
            "h": "%#I",  # 12-hour format (no leading zero, Windows)
            "HH": "%H",  # 24-hour format
            "ss": "%S",
            "AM/PM": "%p",
            "am/pm":"%p",# AM or PM,
            "MM": "%M",  # Minute with leading zero
            "M": "%#M",  # Minute without leading zero (if needed, Windows-specific)
        }

        # Sort keys in descending order of length to avoid partial replacements
        keys_sorted = sorted(format_mapping.keys(), key=len, reverse=True)
        placeholders = {}

        for idx, key in enumerate(keys_sorted):
            if key in format_string:
                placeholder = f"__{idx}__"
                format_string = format_string.replace(key, placeholder)
                placeholders[placeholder] = format_mapping[key]

        # Replace placeholders
        for placeholder, value in placeholders.items():
            format_string = format_string.replace(placeholder, value)

        return format_string

    except Exception as ex:
        raise Exception(str(ex))
def is_valid_date(date):
    try:
        if isinstance(date, str):
            parser.parse(date)
            return True

        elif isinstance(date, list):
            results = []
            for dat in date:
                if not isinstance(dat, str):
                    results.append(False)
                else:
                    try:
                        parser.parse(dat)
                        results.append(True)
                    except ValueError:
                        results.append(False)
            return results
        elif isinstance(date,datetime.datetime):
            return True
    except ValueError:
        return False


def excel_to_python_format(excel_format):
    if excel_format.startswith('$'):
        # Currency format
        if '_' in excel_format:
            return re.sub(r'\$([^_]+)_(.+)', r'${:,\1}_);\($\2)', excel_format)
        else:
            decimal_places = len(excel_format.split('.')[1])
            format_string = '${:,.%df}' % decimal_places
            return re.sub(r'\$([^_]+)', format_string, excel_format)
    elif excel_format.endswith('%'):
        pattern = r'(\d*\.?\d*)%'
        match = re.search(pattern, excel_format)
        if match:
            value = match.group(1)
            decimal_places = len(value) - 2
            replacement = '{{:.{}%}}'.format(decimal_places)
            result = re.sub(pattern, replacement, "0.00%")
        # Percentage format
            return result
    elif ',' in excel_format:
        # Thousand separator format
        return re.sub(r',', r'{:,}', excel_format)
    else:
        # Default numeric format
        # return re.sub(r'\d*\.?\d+', r'{:0\1f}', excel_format)
        pattern = r'\d*\.?\d+'
        match = re.search(pattern, excel_format)
        if match:
            value = match.group(1)
            decimal_places = len(value) - 2
            replacement = '{{:.{}f}}'.format(decimal_places)
            result = re.sub(pattern, replacement, "0.00")
            return result


def is_numeric_format(format_string):
    numeric_patterns = [
        r"\d+(\.\d+)?%",  # Percentage format
        r"\$\s*\d+(\.\d+)?",  # Currency format
        r"\d+(,\d+)?(\.\d+)?",  # Number format with commas
        r"\d{1,3}(,\d{3})*(\.\d+)?",
        r"\w*\s*[^\d\s]\s*\d+(\.\d+)?",
        r"\d*[#?]\d*",  # Number format with '#' or '?' characters
        r",",
        r'[\$\€\£]?[ #.0,9E+-]+'
    ]

    for pattern in numeric_patterns:
        if not format_string == '# ?/?' and re.match(pattern, format_string):
            return True
    return False

#
# def is_date_format(format_string):
#
#     date_formats_pattern = [r"(\b(?:[mdy]+[.-/]*){3,}\b)"]
#
#     for pattern in date_formats_pattern:
#         if not format_string == '# ?/?' and re.match(pattern, format_string):
#             return True
#     return False


def is_date_format(format_string):
    date_patterns = [
        r"([mdyMDY]+\W*){2,3}",  # Date format with 'm', 'd', 'y' characters
        r"[HhMms]+",  # Time format with 'H', 'M', 's' characters
    ]
    for pattern in date_patterns:
        if re.match(pattern, format_string):
            return True
    return False


def SUMOFF(value, rolling_index, offset):
    try:
        sums = []
        if isinstance(value,pd.Series):
            for i in range(len(value)):
                start_index = i + rolling_index
                end_index = start_index + offset
                if start_index < 0:
                    start_index = 0
                if end_index > len(value):
                    end_index = len(value)
                total_sum = sum(value[start_index:end_index])
                sums.append(total_sum)
            return sums
        else:
            return 'Invalid expression'
    except Exception as ex:
        raise Exception(str(ex))


def SUMFIX(formula, finalDict, dataframes, currentDf):
    try:
        args = parse_formula(formula)
        combined_df = concat_dataframes(dataframes)
        value_df_name, value_col = extract_df_col(args[0])
        silicer_name, silicer_col = extract_df_col(args[1])
        group_df_name, group_col = extract_df_col(args[2])
        if isinstance(eval(args[0]), pd.Series):
            result = combined_df.groupby([f"{group_df_name}_{group_col}"])[f"{value_df_name}_{value_col}"].transform('sum')
            return result
        else:
            return "Invalid expression"
    except Exception as ex:
        raise Exception(str(ex))

def LOOKUP(lookup_value, lookup_array, return_array, if_not_found='#N/A', match_mode=0, return_mode=1, delimiter=','):
    try:
        return evaluate_lookup(lookup_value, lookup_array, return_array, if_not_found, match_mode, return_mode, delimiter)
    except Exception as ex:
        raise Exception(str(ex))


def evaluate_lookup(lookup_value, lookup_array, return_array, if_not_found, match_mode, return_mode, delimiter):
    try:
        # Creating dataframes
        single_return = False
        value_name = 'LOOKUP_VALUE'
        if isinstance(lookup_value, pd.Series):
            primary_df = pd.DataFrame({value_name: lookup_value})
        else:
            single_return = True
            data = {value_name: [lookup_value]}
            primary_df = pd.DataFrame(data)
        array_name = 'LOOKUP_ARRAY'
        return_name = 'RETURN_ARRAY'
        secondary_df = pd.DataFrame({array_name: lookup_array, return_name: return_array})
        # Added extra column to return data in original case if lookup array and return array are same.
        secondary_df["lookupReturnOriginal"] = secondary_df[return_name]

        if primary_df[value_name].dtype == 'object' or secondary_df[array_name].dtype == 'object':
            primary_df[value_name] = primary_df[value_name].astype(str).str.upper()
            secondary_df[array_name] = secondary_df[array_name].astype(str).str.upper()

        if array_name not in secondary_df.columns:
            raise ValueError('Invalid lookup array')
        if return_name.upper() not in secondary_df.columns:
            raise ValueError('Invalid return column')
        if match_mode not in [-1, 0, 1, 2]:
            raise ValueError('Invalid match mode')
        if return_mode not in [-1, 1, 2, 3]:
            raise ValueError('Invalid return mode')

        # reversing dataframe for bottom to top searching.
        if return_mode == -1:
            secondary_df = secondary_df[::-1]

        if match_mode == 0:  # Exact match
            if return_mode in [-1, 1]:  # First or last match
                merged = pd.merge(primary_df[[value_name]],
                                  secondary_df[[array_name, "lookupReturnOriginal"]].drop_duplicates(subset=array_name, keep='first'),
                                  left_on=value_name, right_on=array_name, how='left')
                result = merged.iloc[:, -1].fillna(if_not_found).tolist()
            elif return_mode == 2:  # Multi value return
                grouped_secondary_df = secondary_df.groupby(array_name)["lookupReturnOriginal"].apply(lambda x: delimiter.join(x.unique())).reset_index()
                merged = pd.merge(primary_df[[value_name]], grouped_secondary_df[[array_name, "lookupReturnOriginal"]],
                                  left_on=value_name, right_on=array_name, how='left')
                result = merged.iloc[:, -1].fillna(if_not_found).tolist()

            elif return_mode == 3:  # Count return
                counted_df = secondary_df.groupby(array_name)["lookupReturnOriginal"].count().reset_index()
                merged = pd.merge(primary_df[[value_name]], counted_df[[array_name, "lookupReturnOriginal"]],
                                  left_on=value_name, right_on=array_name, how='left')
                result = merged.iloc[:, -1].apply(lambda x: int(x) if pd.notna(x) else if_not_found).tolist()

        elif match_mode == -1:  # Next smallest item if no exact match
            primary_df['__order__'] = range(len(primary_df))
            if return_mode in [-1, 1]:
                # Handle string matching
                if primary_df[value_name].dtype == 'object' and secondary_df[array_name].dtype == 'object':
                    # Sort both DataFrames by the relevant string column
                    primary_sorted = primary_df[[value_name, '__order__']].sort_values(value_name).reset_index(drop=True)
                    secondary_sorted = secondary_df[[array_name, "lookupReturnOriginal"]].sort_values(
                        array_name).reset_index(drop=True)

                    # Custom logic to find the next smallest string for each value in primary_df
                    result = []
                    for val in primary_sorted[value_name]:
                        # Find the largest string in secondary_sorted that's <= the current val
                        match = secondary_sorted[secondary_sorted[array_name] <= val]
                        if not match.empty:
                            closest_value = match[array_name].values[-1]
                            result.append(
                                match.loc[match[array_name] == closest_value, 'lookupReturnOriginal'].values[0])
                        else:
                            result.append(if_not_found)  # Use fallback value if no match is found

                    # Restore the original order of the primary DataFrame
                    primary_df['result'] = result
                    primary_df.sort_values('__order__', inplace=True)
                    result = primary_df['result'].values if len(primary_df) > 1 else primary_df['result'].values[0]

                else:
                    # If the types are numeric, continue with the original merge_asof logic
                    merged = pd.merge_asof(
                        primary_df[[value_name, '__order__']].sort_values(value_name),
                        secondary_df[[array_name, "lookupReturnOriginal"]].sort_values(array_name),
                        left_on=value_name, right_on=array_name, direction='backward'
                    )
                    merged.sort_values('__order__', inplace=True)
                    result = merged.iloc[:, -1].fillna(if_not_found).tolist()

        elif match_mode == 1:  # Next largest item if no exact match
            primary_df['__order__'] = range(len(primary_df))
            if return_mode in [-1, 1]:
                # Handle string matching
                if primary_df[value_name].dtype == 'object' and secondary_df[array_name].dtype == 'object':
                    # Sort both DataFrames by the relevant string column
                    primary_sorted = primary_df[[value_name, '__order__']].sort_values(value_name).reset_index(drop=True)
                    secondary_sorted = secondary_df[[array_name, "lookupReturnOriginal"]].sort_values(
                        array_name).reset_index(drop=True)

                    # Custom logic to find the next largest string for each value in primary_df
                    result = []
                    for val in primary_sorted[value_name]:
                        # Find the smallest string in secondary_sorted that's >= the current val
                        match = secondary_sorted[secondary_sorted[array_name] >= val]
                        if not match.empty:
                            closest_value = match[array_name].values[0]
                            result.append(
                                match.loc[match[array_name] == closest_value, 'lookupReturnOriginal'].values[0])
                        else:
                            result.append(if_not_found)  # Use fallback value if no match is found

                    # Restore the original order of the primary DataFrame
                    primary_df['result'] = result
                    primary_df.sort_values('__order__', inplace=True)
                    result = primary_df['result'].values if len(primary_df) > 1 else primary_df['result'].values[0]

                else:
                    # If the types are numeric, continue with the original merge_asof logic
                    merged = pd.merge_asof(
                        primary_df[[value_name, '__order__']].sort_values(value_name),
                        secondary_df[[array_name, "lookupReturnOriginal"]].sort_values(array_name),
                        left_on=value_name, right_on=array_name, direction='forward'
                    )
                    merged.sort_values('__order__', inplace=True)
                    result = merged.iloc[:, -1].fillna(if_not_found).tolist()

        elif match_mode == 2:  # Wildcard match
            pattern = lookup_value.upper().replace('~?', '__QUESTION_MARK__').replace('~*', '__ASTERISK__') \
                .replace('?', '.').replace('*', '.*').replace('__QUESTION_MARK__', '\?').replace('__ASTERISK__', '\*')
            try:
                # result = secondary_df[array_name].str.extract(fr'^({pattern})$', expand=False).dropna().iloc[0]
                matches = secondary_df[secondary_df[array_name].str.extract(fr'^({pattern})$', expand=False).notna()]
                if not matches.empty:
                    result = matches["lookupReturnOriginal"].iloc[0]  # Get the first matching return array value
                else:
                    result = if_not_found
            except IndexError:
                result = if_not_found
        return result[0] if single_return else result
    except Exception as ex:
        raise Exception(str(ex))


def lookupbyrow(lookup_value, lookup_array, return_array, if_not_found, secondary_df):
    try:
        result = secondary_df.loc[secondary_df[lookup_array].str.upper() == lookup_value.upper(), return_array].iloc[0]
        return result
    except IndexError:
        return if_not_found


def ISNUMBER(value):
    try:
        if isinstance(value, (str, int, float)):
            try:
                if not pd.isna(value):
                    float(value)
                    return True
                return False
            except ValueError:
                return False
        elif isinstance(value, pd.Series):
            return value.apply(lambda x: ISNUMBER(x))
    except Exception as ex:
        raise Exception(str(ex))


def ISBLANK(value):
    try:
        if isinstance(value, (str, int, float)):
            return str(value) == ""
        elif isinstance(value, pd.Series):
            return value.apply(lambda x: is_blank(x))
    except Exception as ex:
        raise Exception(str(ex))


def is_blank(val):
    try:
        if pd.isna(val):
            return True
        return False
    except Exception as ex:
        raise Exception(str(ex))


def SEARCH(find_text, within_text, start_num=1):
    try:
        # Adjust start_num to start from 0 index
        start_num -= 1
        find_text = find_text.replace('~?', '__QUESTION_MARK__').replace('~*', '__ASTERISK__').replace('?', '.').replace('*', '.*').replace('__QUESTION_MARK__', '\?').replace('__ASTERISK__', '\*')
        if isinstance(within_text, str):
            # Use re.IGNORECASE flag for case-insensitive search
            match = re.search(find_text, within_text[start_num:], re.IGNORECASE)
            if match:
                return match.start() + start_num + 1
            else:
                # return "#VALUE!"
                return np.nan
        elif isinstance(within_text, (int, float)):
            return str(within_text).find(find_text, start_num) + 1
        elif isinstance(within_text, pd.Series):
            return within_text.astype(str).apply(lambda x: re.search(find_text, x[start_num:], re.IGNORECASE).start() + 1 if re.search(find_text, x[start_num:], re.IGNORECASE) else np.nan)
        # elif isinstance(within_text, pd.Series):
        #     return within_text.astype(str).apply(lambda x: re.search(find_text, x, re.IGNORECASE).start() + 1 if re.search(find_text, x, re.IGNORECASE) else -1)
        else:
            return within_text
    except Exception as ex:
        raise Exception(str(ex))


# Function to convert a datetime to an Excel serial number
def date_to_excel_serial(dt):
    if isinstance(dt, datetime.datetime):
        delta = dt - datetime.datetime(1899, 12, 30)  # Excel's base date (with correction for leap year bug)
        return float(delta.days) + (delta.seconds / 86400)
    return dt  # Return the original value if not a datetime


# Helper to check if all arguments are dates
def are_all_dates(lst):
    return all(isinstance(x, (datetime.datetime, pd.Timestamp)) for x in lst if x is not None)


# Helper to filter out non-numeric and non-date values
def filter_valid_values(lst):
    lst = pd.Series(lst).dropna().tolist()
    return [x for x in lst if isinstance(x, (int, float, datetime.datetime, pd.Timestamp))]


# If the argument is scalar (numeric or date), wrap it in a list
def ensure_iterable(arg, length=None):
    if isinstance(arg, (int, float, datetime.datetime, pd.Timestamp)):
        # return [arg] * length if length else [arg]  # Treat scalar as a single-item list, broadcast it if length provided
        return pd.Series([arg] * (length or 1))
    elif isinstance(arg, str):
        try:
            # Try to convert to a number
            arg_as_number = float(arg) if '.' in arg else int(arg)
            # return [arg_as_number] * length if length else [arg_as_number]
            return pd.Series([arg_as_number] * (length or 1))
        except ValueError:
            pass  # Not a number, continue to check for date

        try:
            # Try to parse as a date (using ISO format or flexible parsing)
            arg_as_date = pd.to_datetime(arg, errors='raise')
            # return [arg_as_date] * length if length else [arg_as_date]
            return pd.Series([arg_as_date] * (length or 1))
        except ValueError:
            pass  # Not a date, leave as string
    return arg  # Return the original argument if it's iterable


def MIN(*args):
    try:
        # Determine the length to broadcast scalars if mixed with lists/series
        max_length = max([len(arg) if isinstance(arg, (list, np.ndarray, pd.Series)) else 1 for arg in args])

        # Ensure all arguments are iterable and scalars are broadcasted properly
        args = [ensure_iterable(arg, max_length) for arg in args]

        # If there's only one argument, calculate the minimum for that argument
        if len(args) == 1:
            arg = args[0]
            if isinstance(arg, pd.Series):
                arg = arg.dropna()
                # If all values are dates, return the minimum date
                if are_all_dates(arg):
                    return min(arg)
                # Otherwise, filter valid numeric values and calculate min
                else:
                    valid_values = filter_valid_values(arg)
                    if valid_values:
                        return min([date_to_excel_serial(x) for x in valid_values])
                    return 0

        # If there are multiple arguments, check if all are dates and handle accordingly
        else:
            if all(are_all_dates(arg) for arg in args):
                df = pd.DataFrame(args).T
                result = df.apply(lambda row: row.min(skipna=True), axis=1).tolist()
                return result
                # Row-wise minimum for dates
                # return [min(row) for row in zip(*args)]

            rows = []
            for row in zip(*args):  # Unpacking arguments to rows
                valid_values = filter_valid_values(row)
                if valid_values:
                    # Check if the row contains dates, convert dates to serial numbers
                    serial_values = [date_to_excel_serial(x) for x in valid_values]
                    rows.append(min(serial_values))
                else:
                    rows.append(0)  # If no valid numbers, append None
            return rows
    except Exception as ex:
        raise Exception(str(ex))


def MAX(*args):
    try:
        # Determine the length to broadcast scalars if mixed with lists/series
        max_length = max([len(arg) if isinstance(arg, (list, np.ndarray, pd.Series)) else 1 for arg in args])

        # Ensure all arguments are iterable and scalars are broadcasted properly
        args = [ensure_iterable(arg, max_length) for arg in args]

        # If there's only one argument, calculate the minimum for that argument
        if len(args) == 1:
            arg = args[0]
            if isinstance(arg, pd.Series):
                arg = arg.dropna()
                # If all values are dates, return the minimum date
                if are_all_dates(arg):
                    return max(arg)
                # Otherwise, filter valid numeric values and calculate min
                else:
                    valid_values = filter_valid_values(arg)
                    if valid_values:
                        return max([date_to_excel_serial(x) for x in valid_values])
                    return 0

        # If there are multiple arguments, check if all are dates and handle accordingly
        else:
            if all(are_all_dates(arg) for arg in args):
                df = pd.DataFrame(args).T
                result = df.apply(lambda row: row.max(skipna=True), axis=1).tolist()
                return result
                # Row-wise minimum for dates
                # return [max(row) for row in zip(*args)]

            rows = []
            for row in zip(*args):  # Unpacking arguments to rows
                valid_values = filter_valid_values(row)
                if valid_values:
                    # Check if the row contains dates, convert dates to serial numbers
                    serial_values = [date_to_excel_serial(x) for x in valid_values]
                    rows.append(max(serial_values))
                else:
                    rows.append(0)  # If no valid numbers, append None
            return rows
    except Exception as ex:
        raise Exception(str(ex))


def SUMIFS(formula, finalDict, dataframes, currentDf, currentDataModel):
    try:
        args = parse_formula(formula)
        if len(args) % 2 != 1:
            raise ValueError("Invalid Arguments!")
        sum_df_name, sum_col = extract_df_col(args[0])
        sum_col_name = f"{sum_df_name}_{sum_col}"
        groupby_list = []
        cond_col_list = []
        operatorlist = ["<", ">", "<=", ">=", "!=",]
        combined_df = concat_dataframes(dataframes)
        # combined_df = pd.concat([combined_df, currentDf], axis=1)
        sum_col_length = len(dataframes[sum_df_name][sum_col])
        conditions = []
        for i in range(1, len(args) - 1, 2):
            if args[i].startswith("dataframes["):
                range_df_name, range_col = extract_df_col(args[i])
                range_col_name = f"{range_df_name}_{range_col}"
                # groupby_list.append(range_col_name)
                if len(dataframes[range_df_name][range_col]) != sum_col_length:
                    return "#VALUE!"
            elif args[i].startswith("finalDict["):
                series = pd.Series(eval(args[i]), name=f"temp_col_{i}")
                if len(series) != sum_col_length:
                    return "#VALUE!"
                combined_df = pd.concat([combined_df, series], axis=1)
                range_col_name = series.name
                # groupby_list.append(range_col_name)
            else:
                raise ValueError("Invalid range reference")

            condition = args[i + 1]
            if condition.startswith("dataframes["):
                cond_df_name, cond_col_name = extract_df_col(condition)
                cond_col_name = f"{cond_df_name}_{cond_col_name}"
                unique_values = combined_df[cond_col_name].dropna().unique()
                conditions.append(f"`{range_col_name}` in {list(unique_values)}")
                cond_col_list.append(cond_col_name)
                groupby_list.append(range_col_name)
            elif condition.startswith("finalDict["):
                temp_cond = eval(condition)
                if isinstance(temp_cond, pd.Series):
                    name = f"temp_col_{i+1}"
                    series = pd.Series(temp_cond, name=name)
                    combined_df = pd.concat([combined_df, series], axis=1)
                    unique_values = series.unique()
                    conditions.append(f"`{range_col_name}` in {list(unique_values)}")
                    cond_col_list.append(name)
                    groupby_list.append(range_col_name)
                else:
                    if isinstance(temp_cond, (int, float)):
                        conditions.append(f"`{range_col_name}` == {temp_cond}")
                    elif condition[0] in operatorlist:
                        conditions.append(f"{range_col_name} {temp_cond}")
                    else:
                        conditions.append(f"`{range_col_name}` == '{temp_cond}'")
                    # cond_col_list.append(range_col_name)
            else:
                if is_numeric_column(combined_df, range_col_name):
                    condition = convert_to_numeric(condition)
                if isinstance(condition, (int, float)):
                    conditions.append(f"`{range_col_name}` == {condition}")
                elif condition[0] in operatorlist:
                    conditions.append(f"{range_col_name} {condition}")
                else:
                    conditions.append(f"`{range_col_name}` == '{condition}'")
                # cond_col_list.append(range_col_name)
        combined_df1 = combined_df.copy()
        filtered_df = apply_conditions(combined_df, conditions)
        if groupby_list:
            result = filtered_df.groupby(groupby_list)[sum_col_name].sum().reset_index()
            result = pd.merge(combined_df1, result, left_on=cond_col_list, right_on=groupby_list, how='left')[
                f"{sum_col_name}_y"]
            result = result.iloc[:len(currentDf)].fillna(0)
        else:
            result = filtered_df[sum_col_name].sum()
        return result
    except Exception as ex:
        raise Exception(str(ex))


def AVERAGEIFS(formula, finalDict, dataframes, currentDf, currentDataModel):
    try:
        args = parse_formula(formula)
        if len(args) % 2 != 1:
            raise ValueError("Invalid Arguments!")
        sum_df_name, sum_col = extract_df_col(args[0])
        sum_col_name = f"{sum_df_name}_{sum_col}"
        groupby_list = []
        cond_col_list = []
        operatorlist = ["<", ">", "<=", ">=", "!=",]
        combined_df = concat_dataframes(dataframes)
        # combined_df = pd.concat([combined_df, currentDf], axis=1)
        sum_col_length = len(dataframes[sum_df_name][sum_col])
        conditions = []
        for i in range(1, len(args) - 1, 2):
            if args[i].startswith("dataframes["):
                range_df_name, range_col = extract_df_col(args[i])
                range_col_name = f"{range_df_name}_{range_col}"
                # groupby_list.append(range_col_name)
                if len(dataframes[range_df_name][range_col]) != sum_col_length:
                    return "#VALUE!"
            elif args[i].startswith("finalDict["):
                series = pd.Series(eval(args[i]), name=f"temp_col_{i}")
                if len(series) != sum_col_length:
                    return "#VALUE!"
                combined_df = pd.concat([combined_df, series], axis=1)
                range_col_name = series.name
                # groupby_list.append(range_col_name)
            else:
                raise ValueError("Invalid range reference")

            condition = args[i + 1]
            if condition.startswith("dataframes["):
                cond_df_name, cond_col_name = extract_df_col(condition)
                cond_col_name = f"{cond_df_name}_{cond_col_name}"
                unique_values = combined_df[cond_col_name].dropna().unique()
                conditions.append(f"`{range_col_name}` in {list(unique_values)}")
                cond_col_list.append(cond_col_name)
                groupby_list.append(range_col_name)
            elif condition.startswith("finalDict["):
                temp_cond = eval(condition)
                if isinstance(temp_cond, pd.Series):
                    name = f"temp_col_{i+1}"
                    series = pd.Series(temp_cond, name=name)
                    combined_df = pd.concat([combined_df, series], axis=1)
                    unique_values = series.unique()
                    conditions.append(f"`{range_col_name}` in {list(unique_values)}")
                    cond_col_list.append(name)
                    groupby_list.append(range_col_name)
                else:
                    if isinstance(temp_cond, (int, float)):
                        conditions.append(f"`{range_col_name}` == {temp_cond}")
                    elif condition[0] in operatorlist:
                        conditions.append(f"{range_col_name} {temp_cond}")
                    else:
                        conditions.append(f"`{range_col_name}` == '{temp_cond}'")
                    # cond_col_list.append(range_col_name)
            else:
                if is_numeric_column(combined_df, range_col_name):
                    condition = convert_to_numeric(condition)
                if isinstance(condition, (int, float)):
                    conditions.append(f"`{range_col_name}` == {condition}")
                elif condition[0] in operatorlist:
                    conditions.append(f"{range_col_name} {condition}")
                else:
                    conditions.append(f"`{range_col_name}` == '{condition}'")
                # cond_col_list.append(range_col_name)
        combined_df1 = combined_df.copy()
        filtered_df = apply_conditions(combined_df, conditions)
        if groupby_list:
            result = filtered_df.groupby(groupby_list)[sum_col_name].mean().reset_index()
            result = pd.merge(combined_df1, result, left_on=cond_col_list, right_on=groupby_list, how='left')[
                f"{sum_col_name}_y"]
            result = result.iloc[:len(currentDf)].fillna(0)
        else:
            result = filtered_df[sum_col_name].mean()
        return result
    except Exception as ex:
        raise Exception(str(ex))


def COUNTIFS(formula, finalDict, dataframes, currentDf, currentDataModel):
    try:
        operatorlist = ["<", ">", "<=", ">=", "!=", "="]
        args = parse_formula(formula)
        if len(args) % 2 != 0:
            raise ValueError("Invalid Arguments!")

        combined_df = concat_dataframes(dataframes)
        for col in combined_df.select_dtypes(include=['datetime64[ns]']).columns:
            combined_df[col] = combined_df[col].astype(str).replace('NaT', "")
        # Validate that the length of all criteria ranges is the same
        length_of_criteria = None
        for i in range(0, len(args) - 1, 2):
            if args[i].startswith("dataframes["):
                range_df_name, range_col_name = extract_df_col(args[i])
                criteria_length = len(dataframes[range_df_name][range_col_name])
            elif args[i].startswith("finalDict["):
                criteria_length = len(eval(args[i]))
            else:
                return "#VALUE!"

            if length_of_criteria is None:
                length_of_criteria = criteria_length
            elif length_of_criteria != criteria_length:
                # raise ValueError("All criteria ranges must have the same length.")
                return "#VALUE!"

        conditions = []
        groupby_columns = []
        cond_col_list = []

        for i in range(0, len(args) - 1, 2):
            if args[i].startswith("dataframes["):
                range_df_name, range_col_name = extract_df_col(args[i])
                range_col_name = f"{range_df_name}_{range_col_name}"
            elif args[i].startswith("finalDict["):
                series = pd.Series(eval(args[i]), name=f"temp_col_{i}")
                combined_df = pd.concat([combined_df, series], axis=1)
                range_col_name = series.name
            else:
                raise ValueError("Invalid range reference")

            condition = args[i + 1]
            if condition.startswith("dataframes["):
                cond_df_name, cond_col_name = extract_df_col(condition)
                cond_col_name = f"{cond_df_name}_{cond_col_name}"
                unique_values = combined_df[cond_col_name].unique()
                # Create condition for non-NaN values
                non_nan_values = [val for val in unique_values if pd.notna(val)]
                condition_str = f"`{range_col_name}` in {list(non_nan_values)}"
                # Check if NaN is in unique values and handle it separately
                if pd.isna(unique_values).any():
                    condition_str = f"({condition_str} or `{range_col_name}`.isna())"
                conditions.append(condition_str)
                cond_col_list.append(cond_col_name)
                groupby_columns.append(range_col_name)
            elif condition.startswith("finalDict["):
                temp_cond = eval(condition)
                if isinstance(temp_cond, pd.Series):
                    name = f"temp_col_{i + 1}"
                    series = pd.Series(temp_cond, name=name)
                    combined_df = pd.concat([combined_df, series], axis=1)
                    unique_values = series.unique()
                    non_nan_values = [val for val in unique_values if pd.notna(val)]
                    condition_str = f"`{range_col_name}` in {list(non_nan_values)}"
                    if pd.isna(unique_values).any():
                        condition_str = f"({condition_str} or `{range_col_name}`.isna())"
                    conditions.append(condition_str)
                    cond_col_list.append(name)
                    groupby_columns.append(range_col_name)
                else:
                    if isinstance(temp_cond, (int, float)):
                        conditions.append(f"`{range_col_name}` == {temp_cond}")
                    elif condition[0] in operatorlist:
                        conditions.append(f"{range_col_name} {temp_cond}")
                    else:
                        conditions.append(f"`{range_col_name}` == '{temp_cond}'")
                    # groupby_columns.append(range_col_name)
            else:
                if is_numeric_column(combined_df, range_col_name):
                    condition = convert_to_numeric(condition)
                if isinstance(condition, (int, float)):
                    conditions.append(f"`{range_col_name}` == {condition}")
                elif condition[0] in operatorlist:
                    conditions.append(f"{range_col_name} {condition}")
                else:
                    conditions.append(f"`{range_col_name}` == '{condition}'")
                # groupby_columns.append(range_col_name)
        # filtered_df = apply_conditions(combined_df, conditions)
        combined_df1 = combined_df.copy()
        filtered_df = apply_conditions(combined_df, conditions)
        if groupby_columns:
            grouped_df = filtered_df.groupby(groupby_columns).size().reset_index(name='count')
            # merged_df = pd.merge(combined_df, grouped_df, on=groupby_columns, how='left')
            result = pd.merge(combined_df1, grouped_df, left_on=cond_col_list, right_on=groupby_columns, how='left')['count']
            # result = result['count']
            result = result.iloc[:len(currentDf)].fillna(0)
        else:
            result = len(filtered_df)
        return result
    except Exception as ex:
        raise Exception(str(ex))


def parse_formula(formula):
    expression = formula[formula.index('(') + 1: formula.rindex(')')]
    pattern = re.compile(r'''((?:[^',]|'[^']*')+)''')
    segments = pattern.findall(expression)
    return [segment.strip().strip("'") for segment in segments]


def extract_df_col(arg):
    df_name = arg.split("[")[1].split("]")[0].strip('"').strip("'")
    col_name = arg.split("[")[2].split("]")[0].strip('"').strip("'")
    return df_name, col_name


def is_numeric_column(df, col_name):
    return pd.api.types.is_numeric_dtype(df[col_name])


def convert_to_numeric(value):
    try:
        return float(value)
    except ValueError:
        return value


def apply_conditions(df, conditions):
    try:
        query_str = " and ".join(conditions)
        return df.query(query_str)
    except Exception as ex:
        try:
            mask = pd.Series(True, index=df.index)

            for cond in conditions:
                cond = cond.strip()

                # Handle `in` clause with regex to avoid splitting incorrectly
                in_match = re.match(r"^`?(.*?)`?\s+in\s+(.+)$", cond)
                if in_match:
                    col, val_list = in_match.groups()
                    col = col.strip()
                    val_list = val_list.strip().strip('"').strip("'")

                    if pd.api.types.is_datetime64_any_dtype(df[col]):
                        val_list = eval(val_list.strip(), {"Timestamp": pd.Timestamp})
                        val_list = pd.to_datetime(val_list)
                    else:
                        val_list = eval(val_list)

                    mask &= df[col].isin(val_list)
                    continue

                # Handle other operators
                else:
                    for op in ["=","==", "!=", ">=", "<=", ">", "<"]:
                        if op in cond:
                            col_expr, val = cond.split(op, 1)
                            col = col_expr.strip("` ")
                            val = val.strip().strip('"').strip("'")

                            if pd.api.types.is_datetime64_any_dtype(df[col]):
                                val = pd.to_datetime(val)

                            mask &= eval(f"df[col] {op} val")
                            break
            return df[mask]
        except Exception as ex:
            raise Exception(ex)



def concat_dataframes(dataframes):
    combined_df = pd.DataFrame()
    for name, df in dataframes.items():
        df_copy = df.copy()
        df_copy.columns = [f"{name}_{col}" for col in df.columns]
        combined_df = pd.concat([combined_df, df_copy], axis=1)
    return combined_df


def EOMONTH(start_date, months):
    try:
        # Convert start_date to pandas datetime, allowing for strings, datetime, and series inputs
        start_date = pd.to_datetime(start_date, errors='coerce')  # Convert invalid dates to NaT

        # Check if start_date is scalar (a single value like string, datetime, or Timestamp)
        if isinstance(start_date, pd.Timestamp) or pd.api.types.is_scalar(start_date):
            # Handle scalar start_date (single date)
            end_date = start_date + pd.DateOffset(months=months)
            eom = end_date + pd.offsets.MonthEnd(0)
        else:
            # Handle Series or array-like start_date (pandas Series, list, etc.)
            if isinstance(months, int):
                # If months is a scalar, broadcast it across the entire Series
                months = pd.Series([months] * len(start_date), index=start_date.index)

            # Check for mismatched Series lengths between start_date and months
            if isinstance(months, pd.Series) and len(start_date) != len(months):
                raise ValueError("Length of months Series must match start_date Series.")

            # Apply the DateOffset to each element in the Series
            end_date = start_date + months.apply(lambda m: pd.DateOffset(months=m))

            # Find the last day of the month for each resulting date
            eom = end_date + pd.offsets.MonthEnd(0)

        return pd.to_datetime(eom)
    except Exception as ex:
        raise Exception(ex)

def TIME(hour, minute, second):
    try:
        return datetime.time(hour, minute, second)
    except Exception as ex:
        raise Exception(ex)



