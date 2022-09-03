import sys
from antlr4 import *
from typing import List

from parser.TeradataLexer import TeradataLexer
from parser.TeradataParser import TeradataParser
from parser.TeradataListener import TeradataListener


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
    print(extractor.getColumnExpr())


if __name__ == '__main__':
    fd = open(sys.argv[1], "r")
    sql = fd.read()
    fd.close()
    extract_tpt_columns(sql)
