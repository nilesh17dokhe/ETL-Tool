import ast
import re

def ParseFormula(formula):
    try:
        formula = replace_commas(formula)
        # Parse the formula into an AST
        tree = ast.parse(formula, mode='eval')

        # Traverse the AST and extract formulas
        visitor = FormulaVisitor()
        visitor.visit(tree)

        # Extract main formula
        main_formula = ast.unparse(tree.body)

        # The extracted formulas will be stored in visitor.formulas
        extracted_formulas = visitor.formulas

        if len(extracted_formulas) == 0:
            extracted_formulas.append(main_formula)

        if len(extracted_formulas) > 0 and extracted_formulas[0] != main_formula:
            extracted_formulas.insert(0, main_formula)

        return extracted_formulas
    except SyntaxError as ex:
        raise SyntaxError("Invalid Formula")
    except Exception as ex:
        raise Exception(ex)

# Define a visitor class to traverse the AST and extract formulas
def replace_commas(input_str):
    # Define the pattern to match double commas with any number of spaces in between
    pattern = r',\s*,'

    # Replace matches with ",None,"
    output_str = re.sub(pattern, ',None,', input_str)

    return output_str
class FormulaVisitor(ast.NodeVisitor):
    def __init__(self):
        self.formulas = []

    def visit_Call(self, node):
        stringFormulas = ["CLEAN", "LTRIM", "RTRIM", "FIND", "LEFT", "LEN", "LOWER", "MID", "REPT", "RIGHT",
                          "SUBSTITUTE", "TRIM", "UPPER", "CONCAT", "TEXT", "SEARCH"]
        MathFormulas = ["AVERAGE", "CEILING", "EXP", "POWER", "ROUNDVAL", "SUMIFS", "ABS", "AVERAGEIF", "COUNT",
                        "COUNTIF", "COUNTIFS", "FACTDOUBLE", "FACT", "FLOOR", "SQRT", "SUM", "SUMIF", "RANK",
                        "AVERAGEIFS", "SUMOFF", "SUMFIX", "MIN", "MAX"]
        dateFormulas = ["DATE", "TODAY", "NOW", "DATEVALUE", "DAY", "MONTH", "HOUR", "MINUTE", "YEAR", "WEEKDAY", "DATEDIFF",
                        "EOMONTH","TIME"]
        StatisticalFormulas = ["MEAN", "MODE", "MEDIAN", "STDEV", "CORREL", "ISNUMBER", "ISBLANK"]
        otherFormulas = ["IF", "MATCH", "INDEX", "XLOOKUP", "VLOOKUP", "LOOKUP"]
        conditionalFormulas = ["IFERROR", "OR", "AND"]
        formulaList = stringFormulas + MathFormulas + otherFormulas + conditionalFormulas + StatisticalFormulas + dateFormulas
        if isinstance(node.func, ast.Name) and node.func.id in formulaList:
            formula = ast.unparse(node)
            self.formulas.append(formula)

        self.generic_visit(node)


def build_ast(node):
    if isinstance(node, ast.BinOp):
        operator = node.op
        left = build_ast(node.left)
        right = build_ast(node.right)
        return {"type": "binop", "operator": operator.__class__.__name__, "left": left, "right": right}
    elif isinstance(node, ast.Name):
        return {"type": "variable", "name": node.id}
    elif isinstance(node, ast.Num):
        return {"type": "number", "value": node.n}
    else:
        raise ValueError(f"Unsupported node type: {type(node)}")


# Define a function to recursively extract formulas from AST nodes
def extract_formulas(node):
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == 'IF':
        condition = node.args[0]
        true_value = node.args[1]
        false_value = extract_formulas(node.args[2]) if len(node.args) > 2 else None
        return true_value if condition else false_value
    if isinstance(node, ast.BinOp):
        left_formula = extract_formulas(node.left)
        right_formula = extract_formulas(node.right)
        return f"({left_formula} {node.op.__class__.__name__} {right_formula})"
    elif isinstance(node, ast.Name):
        return node.id
    elif isinstance(node, ast.Num):
        return str(node.n)
    else:
        raise ValueError(f"Unsupported node type: {type(node)}")
