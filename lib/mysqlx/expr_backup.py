# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.

# MySQL Connector/Python is licensed under the terms of the GPLv2
# <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most
# MySQL Connectors. There are special exceptions to the terms and
# conditions of the GPLv2 as it is applied to this software, see the
# FOSS License Exception
# <http://www.mysql.com/about/legal/licensing/foss-exception.html>.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA

from protobuf.mysqlx_datatypes_pb2 import *
from protobuf.mysqlx_expr_pb2 import *
from protobuf.mysqlx_crud_pb2 import *

import expr_unparser

class TokenType:
    NOT = 1
    AND = 2
    OR = 3
    XOR = 4
    IS = 5
    LPAREN = 6
    RPAREN = 7
    LSQBRACKET = 8
    RSQBRACKET = 9
    BETWEEN = 10
    TRUE = 11
    NULL = 12
    FALSE = 13
    IN = 14
    LIKE = 15
    INTERVAL = 16
    REGEXP = 17
    ESCAPE = 18
    IDENT = 19
    LSTRING = 20
    LNUM = 21
    DOT = 22
    DOLLAR = 23
    COMMA = 24
    EQ = 25
    NE = 26
    GT = 27
    GE = 28
    LT = 29
    LE = 30
    BITAND = 31
    BITOR = 32
    BITXOR = 33
    LSHIFT = 34
    RSHIFT = 35
    PLUS = 36
    MINUS = 37
    MUL = 38
    DIV = 39
    HEX = 40
    BIN = 41
    NEG = 42
    BANG = 43
    MICROSECOND = 44
    SECOND = 45
    MINUTE = 46
    HOUR = 47
    DAY = 48
    WEEK = 49
    MONTH = 50
    QUARTER = 51
    YEAR = 52
    PLACEHOLDER = 53
    DOUBLESTAR = 54
    MOD = 55
    COLON = 56

interval_units = set([
    TokenType.MICROSECOND,
    TokenType.SECOND,
    TokenType.MINUTE,
    TokenType.HOUR,
    TokenType.DAY,
    TokenType.WEEK,
    TokenType.MONTH,
    TokenType.QUARTER,
    TokenType.YEAR])

# map of reserved word to token type
reservedWords = {
        "and":      TokenType.AND,
        "or":       TokenType.OR,
        "xor":      TokenType.XOR,
        "is":       TokenType.IS,
        "not":      TokenType.NOT,
        "like":     TokenType.LIKE,
        "in":       TokenType.IN,
        "regexp":   TokenType.REGEXP,
        "between":  TokenType.BETWEEN,
        "interval": TokenType.INTERVAL,
        "escape":   TokenType.ESCAPE,
        "div":      TokenType.DIV,
        "hex":      TokenType.HEX,
        "bin":      TokenType.BIN,
        "true":     TokenType.TRUE,
        "false":    TokenType.FALSE,
        "null":     TokenType.NULL,
        "second":   TokenType.SECOND,
        "minute":   TokenType.MINUTE,
        "hour":     TokenType.HOUR,
        "day":      TokenType.DAY,
        "week":     TokenType.WEEK,
        "month":    TokenType.MONTH,
        "quarter":  TokenType.QUARTER,
        "year":     TokenType.YEAR,
        "microsecond": TokenType.MICROSECOND}

class Token:
    def __init__(self, type, val):
        self.type = type
        self.val = val

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        if self.type == TokenType.IDENT or self.type == TokenType.LNUM or self.type == TokenType.LSTRING:
            return str(self.type) + "(" + self.val + ")"
        else:
            return str(self.type)

# static protobuf helper functions
def build_null_scalar():
    return Scalar(type=Scalar.V_NULL)

def build_double_scalar(d):
    return Scalar(type=Scalar.V_DOUBLE, v_double=d)

def build_int_scalar(i):
    return Scalar(type=Scalar.V_SINT, v_signed_int=i)

def build_string_scalar(s):
    return Scalar(type=Scalar.V_STRING, v_string=Scalar.String(value=s))

def build_bool_scalar(b):
    s = Scalar()
    s.type = Scalar.V_BOOL
    s.v_bool = b
    return s

def build_literal_expr(anyval):
    e = Expr()
    e.type = Expr.LITERAL
    e.literal.CopyFrom(anyval)
    return e

def build_unary_op(name, param):
    e = Expr()
    e.type = Expr.OPERATOR
    e.operator.name = name
    e.operator.param.add().CopyFrom(param)
    return e

def escape_literal(string):
    return string.replace('"', '""')

class ExprParser:
    def __init__(self, string, allowRelational):
        self.string = string
        self.tokens = []
        self.pos = 0
        self._allowRelationalColumns = allowRelational
        self.lex()

    # convencience checker for lexer
    def next_char_is(self, i, c):
        return i + 1 < len(self.string) and self.string[i + 1] == c

    def lex(self):
        i = 0
        while i < len(self.string):
            c = self.string[i]
            if c.isspace():
                # do nothing
                pass
            elif c.isdigit():
                # numeric literal
                start = i
                while i < len(self.string) and (self.string[i].isdigit() or self.string[i] == "."):
                    # technically we allow more than one "." and let float()'s parsing complain later
                    i = i + 1
                self.tokens.append(Token(TokenType.LNUM, self.string[start:i]))
                if i < len(self.string):
                    # back up if we're not at the end of the string
                    i = i - 1
            elif not (c.isalpha() or c == "_"):
                # non-identifier, e.g. operator or quoted literal
                if c == "?":
                    self.tokens.append(Token(TokenType.PLACEHOLDER, c))
                elif c == ":":
                    self.tokens.append(Token(TokenType.COLON, c))
                elif c == "+":
                    self.tokens.append(Token(TokenType.PLUS, c))
                elif c == "-":
                    self.tokens.append(Token(TokenType.MINUS, c))
                elif c == "*":
                    if self.next_char_is(i, "*"):
                        i = i + 1
                        self.tokens.append(Token(TokenType.DOUBLESTAR, "**"))
                    else:
                        self.tokens.append(Token(TokenType.MUL, c))
                elif c == "/":
                    self.tokens.append(Token(TokenType.DIV, c))
                elif c == "$":
                    self.tokens.append(Token(TokenType.DOLLAR, c))
                elif c == "%":
                    self.tokens.append(Token(TokenType.MOD, c))
                elif c == "=":
                    if self.next_char_is(i, "="):
                        i = i + 1
                    self.tokens.append(Token(TokenType.EQ, "=="))
                elif c == "&":
                    self.tokens.append(Token(TokenType.BITAND, c))
                elif c == "|":
                    self.tokens.append(Token(TokenType.BITOR, c))
                elif c == "(":
                    self.tokens.append(Token(TokenType.LPAREN, c))
                elif c == ")":
                    self.tokens.append(Token(TokenType.RPAREN, c))
                elif c == "[":
                    self.tokens.append(Token(TokenType.LSQBRACKET, c))
                elif c == "]":
                    self.tokens.append(Token(TokenType.RSQBRACKET, c))
                elif c == "~":
                    self.tokens.append(Token(TokenType.NEG, c))
                elif c == ",":
                    self.tokens.append(Token(TokenType.COMMA, c))
                elif c == "!":
                    if self.next_char_is(i, "="):
                        i = i + 1
                        self.tokens.append(Token(TokenType.NE, "!="))
                    else:
                        self.tokens.append(Token(TokenType.BANG, c))
                elif c == "<":
                    if self.next_char_is(i, "<"):
                        i = i + 1
                        self.tokens.append(Token(TokenType.LSHIFT, "<<"))
                    elif self.next_char_is(i, "="):
                        i = i + 1
                        self.tokens.append(Token(TokenType.LE, "<="))
                    else:
                        self.tokens.append(Token(TokenType.LT, c))
                elif c == ">":
                    if self.next_char_is(i, ">"):
                        i = i + 1
                        self.tokens.append(Token(TokenType.RSHIFT, ">>"))
                    elif self.next_char_is(i, "="):
                        i = i + 1
                        self.tokens.append(Token(TokenType.GE, ">="))
                    else:
                        self.tokens.append(Token(TokenType.GT, c))
                elif c == ".":
                    if i + 1 < len(self.string) and self.string[i + 1].isdigit():
                        # could be a floating point, like .1
                        start = i
                        i = i + 1
                        while i < len(self.string) and self.string[i].isdigit():
                            i = i + 1
                        self.tokens.append(Token(TokenType.LNUM, self.string[start:i]))
                        if i < len(self.string):
                            # back up if we're not at the end of the string
                            i = i - 1
                    else:
                        self.tokens.append(Token(TokenType.DOT, c))
                elif c == '"' or c == "'" or c == "`":
                    quote_char = c
                    val = ""
                    i = i + 1
                    start = i
                    while i < len(self.string):
                        c = self.string[i]
                        if c == quote_char and i + 1 < len(self.string) and self.string[i + 1] != quote_char:
                            # break if we have a quote char that's not double
                            break
                        elif c == quote_char or c == "\\":
                            # this quote char has to be doubled
                            if i + 1 >= len(self.string):
                                break
                            i = i + 1
                            val = val + self.string[i]
                        else:
                            val = val + c
                        i = i + 1
                    if i >= len(self.string) or self.string[i] != quote_char:
                        raise StandardError("Unterminated quoted string starting at " + str(start))
                    if quote_char == "`":
                        self.tokens.append(Token(TokenType.IDENT, val))
                    else:
                        self.tokens.append(Token(TokenType.LSTRING, val))
                else:
                    raise StandardError("Unknown character at " + str(i))
            else:
                start = i
                while i < len(self.string) and (self.string[i].isalnum() or self.string[i] == "_"):
                    i = i + 1
                val = self.string[start:i]
                try:
                    self.tokens.append(Token(reservedWords[val.lower()], val.upper()))
                except KeyError:
                    self.tokens.append(Token(TokenType.IDENT, val))
                if i < len(self.string):
                    # we went one past the last ident char (unless it's end of string)
                    i = i - 1
            i = i + 1

    def assert_cur_token(self, type):
        if self.pos >= len(self.tokens):
            raise StandardError("Expected token type " + str(type) + " at pos " + str(self.pos) + " but no tokens left")
        if self.tokens[self.pos].type != type:
            raise StandardError("Expected token type " + str(type) + " at pos " + str(self.pos) + " but found type " + str(self.tokens[self.pos]))
        pass

    def cur_token_type_is(self, type):
        return self.pos_token_type_is(self.pos, type)

    def next_token_type_is(self, type):
        return self.pos_token_type_is(self.pos + 1, type)

    def pos_token_type_is(self, pos, type):
        return pos < len(self.tokens) and self.tokens[pos].type == type

    def consume_token(self, type):
        self.assert_cur_token(type)
        v = self.tokens[self.pos].val
        self.pos = self.pos + 1
        return v

    def paren_expr_list(self):
        """Parse a paren-bounded expression list for function arguments or IN list and return a list of Expr objects"""
        exprs = []
        self.consume_token(TokenType.LPAREN)
        if not self.cur_token_type_is(TokenType.RPAREN):
            exprs.append(self.expr())
            while self.cur_token_type_is(TokenType.COMMA):
                self.pos = self.pos + 1
                exprs.append(self.expr())
        self.consume_token(TokenType.RPAREN)
        return exprs

    def identifier(self):
        self.assert_cur_token(TokenType.IDENT)
        id = Identifier()
        if self.next_token_type_is(TokenType.DOT):
            id.schema_name = self.consume_token(TokenType.IDENT)
            self.consume_token(TokenType.DOT)
        id.name = self.tokens[self.pos].val
        self.pos = self.pos + 1
        return id

    def function_call(self):
        e = Expr()
        e.type = Expr.FUNC_CALL
        e.function_call.name.CopyFrom(self.identifier())
        e.function_call.param.extend(self.paren_expr_list())
        return e

    def docpath_member(self):
        self.consume_token(TokenType.DOT)
        if self.cur_token_type_is(TokenType.IDENT):
            # TODO: what are the rules for JSON identifiers?
            return "." + self.consume_token(TokenType.IDENT)
        elif self.cur_token_type_is(TokenType.LSTRING):
            return ".\"" + escape_literal(self.consume_token(TokenType.LSTRING)) + "\""
        elif self.cur_token_type_is(TokenType.MUL):
            return "." + self.consume_token(TokenType.MUL)
        else:
            raise StandardError("Expected token type IDENT or LSTRING in JSON path at token pos " + str(self.pos))

    def docpath_array_loc(self):
        self.consume_token(TokenType.LSQBRACKET)
        if self.cur_token_type_is(TokenType.MUL):
            self.consume_token(TokenType.RSQBRACKET)
            return "[*]"
        elif self.cur_token_type_is(TokenType.LNUM):
            v = int(self.consume_token(TokenType.LNUM))
            if v < 0:
                raise StandardError("Array index cannot be negative at " + str(self.pos))
            self.consume_token(TokenType.RSQBRACKET)
            return "[" + str(v) + "]"
        else:
            raise StandardError("Exception token type MUL or LNUM in JSON path array index at token pos " + str(self.pos))

    def document_field(self):
        col = ColumnIdentifier()
        if self.cur_token_type_is(TokenType.IDENT):
            col.document_path.extend([DocumentPathItem(type=DocumentPathItem.MEMBER, value=self.consume_token(TokenType.IDENT))])
        col.document_path.extend(self.document_path())
        return Expr( type=Expr.IDENT, identifier=col )

    def document_path(self):
        """Parse a JSON-style document path, like WL#7909, but prefix by @. instead of $. We parse this as a string because the protocol doesn't support it. (yet)"""
        docpath = ""
        while True:
            if self.cur_token_type_is(TokenType.DOT):
                docpath = docpath + self.docpath_member()
            elif self.cur_token_type_is(TokenType.LSQBRACKET):
                docpath = docpath + self.docpath_array_loc()
            elif self.cur_token_type_is(TokenType.DOUBLESTAR):
                self.consume_token(TokenType.DOUBLESTAR)
                docpath = docpath + "**"
            else:
                break
        if docpath.endswith("**"):
            raise StandardError("JSON path may not end in '**' at " + str(self.pos))
        return docpath

    def column_identifier(self):
        parts = []
        parts.append(self.consume_token(TokenType.IDENT))
        while self.cur_token_type_is(TokenType.DOT):
            self.consume_token(TokenType.DOT)
            parts.append(self.consume_token(TokenType.IDENT))
        if len(parts) > 3:
            raise StandardError("Too many parts to identifier at " + str(self.pos))
        parts.reverse()
        colid = ColumnIdentifier()
        # clever way to apply them to the struct
        for i in xrange(0, len(parts)):
            if i == 0:
                colid.name = parts[0]
            elif i == 1:
                colid.table_name = parts[1]
            elif i == 2:
                colid.schema_name = parts[2]
        if self.cur_token_type_is(TokenType.DOLLAR):
            self.consume_token(TokenType.DOLLAR)
            colid.document_path = self.document_path()
        e = Expr()
        e.type = Expr.IDENT
        e.identifier.CopyFrom(colid)
        return e

    def atomic_expr(self):
        """Parse an atomic expression and return a protobuf Expr object"""
        t = self.tokens[self.pos]
        self.pos = self.pos + 1
        if t.type == TokenType.PLACEHOLDER:
            return build_literal_expr(build_string_scalar("?"))
        elif t.type == TokenType.DOLLAR:
            # TODO: make sure this doesn't interfere with un-prefixed JSON paths
            e = Expr()
            e.type = Expr.VARIABLE
            e.variable = self.consume_token(TokenType.IDENT)
            return e
        elif t.type == TokenType.LPAREN:
            e = self.expr()
            self.consume_token(TokenType.RPAREN)
            return e
        elif self.cur_token_type_is(TokenType.LNUM) and (t.type == TokenType.PLUS or t.type == TokenType.MINUS):
            # add the +/- to the numeric string and loop back through
            self.tokens[self.pos].val = t.val + self.tokens[self.pos].val
            return self.atomic_expr()
        elif t.type == TokenType.PLUS or t.type == TokenType.MINUS or t.type == TokenType.NOT or t.type == TokenType.NEG:
            return build_unary_op(t.val, self.atomic_expr())
        elif t.type == TokenType.LSTRING:
            return build_literal_expr(build_string_scalar(t.val))
        elif t.type == TokenType.NULL:
            return build_literal_expr(build_null_scalar())
        elif t.type == TokenType.LNUM:
            if "." in t.val:
                return build_literal_expr(build_double_scalar(float(t.val)))
            else:
                return build_literal_expr(build_int_scalar(int(t.val)))
        elif t.type == TokenType.TRUE or t.type == TokenType.FALSE:
            return build_literal_expr(build_bool_scalar(t.type == TokenType.TRUE))
        elif t.type == TokenType.INTERVAL:
            e = Expr()
            e.type = Expr.OPERATOR
            e.operator.name = "INTERVAL"
            e.operator.param.add().CopyFrom(self.expr())
            # validate the interval units
            if self.pos < len(self.tokens) and self.tokens[self.pos].type in interval_units:
                pass
            else:
                raise StandardError("Expected interval units at " + str(self.pos))
            e.operator.param.add().CopyFrom(build_literal_expr(build_string_scalar(self.tokens[self.pos].val)))
            self.pos = self.pos + 1
            return e
        elif t.type == TokenType.IDENT:
            self.pos = self.pos - 1 # stay on the identifier
            if self.next_token_type_is(TokenType.LPAREN) or (self.next_token_type_is(TokenType.DOT) and self.pos_token_type_is(self.pos + 2, TokenType.IDENT) and self.pos_token_type_is(self.pos + 3, TokenType.LPAREN)):
                # Function call
                return self.function_call()
            else:
                return self.document_field() if not self._allowRelationalColumns else self.column_identifier()
        raise StandardError("Unknown token type = " + str(t.type) + " when expecting atomic expression at " + str(self.pos))

    def parse_left_assoc_binary_op_expr(self, types, inner_parser):
        """Given a `set' of types and an Expr-returning inner parser function, parse a left associate binary operator expression"""
        lhs = inner_parser()
        while self.pos < len(self.tokens) and self.tokens[self.pos].type in types:
            e = Expr()
            e.type = Expr.OPERATOR
            e.operator.name = self.tokens[self.pos].val
            e.operator.param.add().CopyFrom(lhs)
            self.pos = self.pos + 1
            e.operator.param.add().CopyFrom(inner_parser())
            lhs = e
        return lhs

    # operator precedence is implemented here
    def mul_div_expr(self): return self.parse_left_assoc_binary_op_expr(set([TokenType.MUL, TokenType.DIV, TokenType.MOD]), self.atomic_expr)
    def add_sub_expr(self): return self.parse_left_assoc_binary_op_expr(set([TokenType.PLUS, TokenType.MINUS]), self.mul_div_expr)
    def shift_expr(self): return self.parse_left_assoc_binary_op_expr(set([TokenType.LSHIFT, TokenType.RSHIFT]), self.add_sub_expr)
    def bit_expr(self): return self.parse_left_assoc_binary_op_expr(set([TokenType.BITAND, TokenType.BITOR, TokenType.BITXOR]), self.shift_expr)
    def comp_expr(self): return self.parse_left_assoc_binary_op_expr(set([TokenType.GE, TokenType.GT, TokenType.LE, TokenType.LT, TokenType.EQ, TokenType.NE]), self.bit_expr)
    def ilri_expr(self):
        lhs = self.comp_expr()
        is_not = False
        if self.cur_token_type_is(TokenType.NOT):
            is_not = True
            self.consume_token(TokenType.NOT)
        if self.pos < len(self.tokens):
            params = [lhs]
            op_name = self.tokens[self.pos].val
            if self.cur_token_type_is(TokenType.IS):
                self.consume_token(TokenType.IS)
                # for IS, NOT comes AFTER
                if self.cur_token_type_is(TokenType.NOT):
                    is_not = True
                    self.consume_token(TokenType.NOT)
                params.append(self.comp_expr())
            elif self.cur_token_type_is(TokenType.IN):
                self.consume_token(TokenType.IN)
                params.extend(self.paren_expr_list())
            elif self.cur_token_type_is(TokenType.LIKE):
                self.consume_token(TokenType.LIKE)
                params.append(self.comp_expr())
                if self.cur_token_type_is(TokenType.ESCAPE):
                    self.consume_token(TokenType.ESCAPE)
                    params.append(self.comp_expr())
            elif self.cur_token_type_is(TokenType.BETWEEN):
                self.consume_token(TokenType.BETWEEN)
                params.append(self.comp_expr())
                self.consume_token(TokenType.AND)
                params.append(self.comp_expr())
            elif self.cur_token_type_is(TokenType.REGEXP):
                self.consume_token(TokenType.REGEXP)
                params.append(self.comp_expr())
            else:
                if is_not:
                    raise StandardError("Unknown token after NOT as pos " + str(self.pos))
                op_name = None # not an operator we're interested in
            if op_name:
                e = Expr()
                e.type = Expr.OPERATOR
                e.operator.name = op_name
                e.operator.param.extend(params)
                if is_not:
                    # wrap if `NOT'-prefixed
                    e = build_unary_op("NOT", e)
                lhs = e
        return lhs
    def and_expr(self): return self.parse_left_assoc_binary_op_expr(set([TokenType.AND]), self.ilri_expr)
    def or_expr(self): return self.parse_left_assoc_binary_op_expr(set([TokenType.OR]), self.and_expr)

    def expr(self):
        return self.or_expr()

    def parse_table_insert_field(self):
        return Column(name=self.consume_token(TokenType.IDENT))

def parseAndPrintExpr(expr_string, allowRelational=True):
    print(">>>>>>> parsing:  " + expr_string)
    p = ExprParser(expr_string, allowRelational)
    print(p.tokens)
    e = p.expr()
    print(e)
    #print(expr_unparser.expr_to_string(e))

def x_test():
    parseAndPrintExpr("now () - interval -2 day");
    parseAndPrintExpr("1");
    parseAndPrintExpr("10+1");
    parseAndPrintExpr("(abc == 1)");
    parseAndPrintExpr("(func(abc)=1)");
    parseAndPrintExpr("(abc = \"jess\")");
    parseAndPrintExpr("(abc = \"with \\\"\")");
    parseAndPrintExpr("(abc != .10)");
    parseAndPrintExpr("(abc != \"xyz\")");
    parseAndPrintExpr("a + b * c + d");
    parseAndPrintExpr("(a + b) * c + d");
    parseAndPrintExpr("(field not in ('a',func('b', 2.0),'c'))");
    parseAndPrintExpr("jess.age between 30 and death");
    parseAndPrintExpr("a + b * c + d");
    parseAndPrintExpr("x > 10 and Y >= -20");
    parseAndPrintExpr("a is true and b is null and C + 1 > 40 and (time = now() or hungry())");
    parseAndPrintExpr("a + b + -c > 2");
    parseAndPrintExpr("now () + b + c > 2");
    parseAndPrintExpr("now () + @b + c > 2");
    parseAndPrintExpr("now () - interval +2 day > some_other_time() or something_else IS NOT NULL");
    parseAndPrintExpr("\"two quotes to one\"\"\"");
    parseAndPrintExpr("'two quotes to one'''");
    parseAndPrintExpr("'different quote \"'");
    parseAndPrintExpr("\"different quote '\"");
    parseAndPrintExpr("`ident`");
    parseAndPrintExpr("`ident```");
    parseAndPrintExpr("`ident\"'`");
    parseAndPrintExpr("? > x and func(?, ?, ?)");
    parseAndPrintExpr("a > now() + interval (2 + x) MiNuTe");
    parseAndPrintExpr("a between 1 and 2");
    parseAndPrintExpr("a not between 1 and 2");
    parseAndPrintExpr("a in (1,2,a.b(3),4,5,x)");
    parseAndPrintExpr("a not in (1,2,3,4,5,@x)");
    parseAndPrintExpr("a like b escape c");
    parseAndPrintExpr("a not like b escape c");
    parseAndPrintExpr("(1 + 3) in (3, 4, 5)");
    parseAndPrintExpr("`a crazy \"function\"``'name'`(1 + 3) in (3, 4, 5)");
    parseAndPrintExpr("`a crazy \"function\"``'name'`(1 + 3) in (3, 4, 5)");
    parseAndPrintExpr("a@.b", False);
    parseAndPrintExpr("a@.b[0][0].c**.d.\"a weird\\\"key name\"", False);
    parseAndPrintExpr("a@.*", False);
    parseAndPrintExpr("a@[0].*", False);
    parseAndPrintExpr("a@**[0].*", False);

#x_test()
