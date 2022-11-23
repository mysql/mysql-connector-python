# Copyright (c) 2016, 2022, Oracle and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0, as
# published by the Free Software Foundation.
#
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an
# additional permission to link the program and your derivative works
# with the separately licensed software that they have included with
# MySQL.
#
# Without limiting anything contained in the foregoing, this file,
# which is part of MySQL Connector/Python, is also subject to the
# Universal FOSS Exception, version 1.0, a copy of which can be found at
# http://oss.oracle.com/licenses/universal-foss-exception.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

"""Expression Parser."""

from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Union

from .dbdoc import DbDoc
from .helpers import BYTE_TYPES, get_item_or_attr
from .protobuf import Message, mysqlxpb_enum
from .types import (
    BuildExprTypes,
    BuildScalarTypes,
    MessageType,
    ProtobufMessageCextType,
    ProtobufMessageType,
    StrOrBytes,
)


# pylint: disable=missing-function-docstring
class TokenType:
    """Token types class."""

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
    EROTEME = 53
    DOUBLESTAR = 54
    MOD = 55
    COLON = 56
    OROR = 57
    ANDAND = 58
    LCURLY = 59
    RCURLY = 60
    CAST = 61
    DOTSTAR = 62
    ORDERBY_ASC = 63
    ORDERBY_DESC = 64
    AS = 65
    ARROW = 66
    QUOTE = 67
    BINARY = 68
    DATETIME = 69
    TIME = 70
    CHAR = 71
    DATE = 72
    DECIMAL = 73
    SIGNED = 74
    INTEGER = 75
    UNSIGNED = 76
    JSON = 77
    SECOND_MICROSECOND = 78
    MINUTE_MICROSECOND = 79
    MINUTE_SECOND = 80
    HOUR_MICROSECOND = 81
    HOUR_SECOND = 82
    HOUR_MINUTE = 83
    DAY_MICROSECOND = 84
    DAY_SECOND = 85
    DAY_MINUTE = 86
    DAY_HOUR = 87
    YEAR_MONTH = 88
    OVERLAPS = 89


_INTERVAL_UNITS = set(
    [
        TokenType.MICROSECOND,
        TokenType.SECOND,
        TokenType.MINUTE,
        TokenType.HOUR,
        TokenType.DAY,
        TokenType.WEEK,
        TokenType.MONTH,
        TokenType.QUARTER,
        TokenType.YEAR,
        TokenType.SECOND_MICROSECOND,
        TokenType.MINUTE_MICROSECOND,
        TokenType.MINUTE_SECOND,
        TokenType.HOUR_MICROSECOND,
        TokenType.HOUR_SECOND,
        TokenType.HOUR_MINUTE,
        TokenType.DAY_MICROSECOND,
        TokenType.DAY_SECOND,
        TokenType.DAY_MINUTE,
        TokenType.DAY_HOUR,
        TokenType.YEAR_MONTH,
    ]
)

# map of reserved word to token type
_RESERVED_WORDS = {
    "and": TokenType.AND,
    "or": TokenType.OR,
    "xor": TokenType.XOR,
    "is": TokenType.IS,
    "not": TokenType.NOT,
    "like": TokenType.LIKE,
    "in": TokenType.IN,
    "overlaps": TokenType.OVERLAPS,
    "regexp": TokenType.REGEXP,
    "between": TokenType.BETWEEN,
    "interval": TokenType.INTERVAL,
    "escape": TokenType.ESCAPE,
    "cast": TokenType.CAST,
    "div": TokenType.DIV,
    "hex": TokenType.HEX,
    "bin": TokenType.BIN,
    "true": TokenType.TRUE,
    "false": TokenType.FALSE,
    "null": TokenType.NULL,
    "second": TokenType.SECOND,
    "minute": TokenType.MINUTE,
    "hour": TokenType.HOUR,
    "day": TokenType.DAY,
    "week": TokenType.WEEK,
    "month": TokenType.MONTH,
    "quarter": TokenType.QUARTER,
    "year": TokenType.YEAR,
    "microsecond": TokenType.MICROSECOND,
    "asc": TokenType.ORDERBY_ASC,
    "desc": TokenType.ORDERBY_DESC,
    "as": TokenType.AS,
    "binary": TokenType.BINARY,
    "datetime": TokenType.DATETIME,
    "time": TokenType.TIME,
    "char": TokenType.CHAR,
    "date": TokenType.DATE,
    "decimal": TokenType.DECIMAL,
    "signed": TokenType.SIGNED,
    "unsigned": TokenType.UNSIGNED,
    "integer": TokenType.INTEGER,
    "json": TokenType.JSON,
    "second_microsecond": TokenType.SECOND_MICROSECOND,
    "minute_microsecond": TokenType.MINUTE_MICROSECOND,
    "minute_second": TokenType.MINUTE_SECOND,
    "hour_microsecond": TokenType.HOUR_MICROSECOND,
    "hour_second": TokenType.HOUR_SECOND,
    "hour_minute": TokenType.HOUR_MINUTE,
    "day_microsecond": TokenType.DAY_MICROSECOND,
    "day_second": TokenType.DAY_SECOND,
    "day_minute": TokenType.DAY_MINUTE,
    "day_hour": TokenType.DAY_HOUR,
    "year_month": TokenType.YEAR_MONTH,
}

_SQL_FUNTION_RESERVED_WORDS_COLLISION = {
    "binary": TokenType.BINARY,
    "cast": TokenType.CAST,
    "char": TokenType.CHAR,
    "date": TokenType.DATE,
    "decimal": TokenType.DECIMAL,
    "signed": TokenType.SIGNED,
    "time": TokenType.TIME,
    "unsigned": TokenType.UNSIGNED,
}

_OPERATORS = {
    "=": "==",
    "==": "==",
    "and": "&&",
    "div": "div",
    "||": "||",
    "or": "||",
    "not": "not",
    "xor": "xor",
    "^": "^",
    "is": "is",
    "between": "between",
    "in": "in",
    "like": "like",
    "!=": "!=",
    "<>": "!=",
    ">": ">",
    ">=": ">=",
    "<": "<",
    "<=": "<=",
    "&": "&",
    "&&": "&&",
    "|": "|",
    "<<": "<<",
    ">>": ">>",
    "+": "+",
    "-": "-",
    "*": "*",
    "/": "/",
    "~": "~",
    "%": "%",
    "cast": "cast",
    "cont_in": "cont_in",
    "overlaps": "overlaps",
}

_UNARY_OPERATORS = {
    "+": "sign_plus",
    "-": "sign_minus",
    "~": "~",
    "not": "not",
    "!": "!",
}

_NEGATION = {
    "is": "is_not",
    "between": "not_between",
    "regexp": "not_regexp",
    "like": "not_like",
    "in": "not_in",
    "cont_in": "not_cont_in",
    "overlaps": "not_overlaps",
}


class Token:
    """Token representation class."""

    def __init__(self, token_type: int, value: str, length: int = 1) -> None:
        self.token_type: int = token_type
        self.value: str = value
        self.length: int = length

    def __repr__(self) -> str:
        return self.__str__()

    def __str__(self) -> str:
        if self.token_type in (
            TokenType.IDENT,
            TokenType.LNUM,
            TokenType.LSTRING,
        ):
            return f"{self.token_type}({self.value})"
        return f"{self.token_type}"


# static protobuf helper functions


def build_expr(value: BuildExprTypes) -> MessageType:
    msg = Message("Mysqlx.Expr.Expr")
    if isinstance(value, (Message)):
        return value
    if isinstance(value, (ExprParser)):
        return value.expr(reparse=True)
    if isinstance(value, (dict, DbDoc)):
        msg["type"] = mysqlxpb_enum("Mysqlx.Expr.Expr.Type.OBJECT")
        msg["object"] = build_object(value).get_message()
    elif isinstance(value, (list, tuple)):
        msg["type"] = mysqlxpb_enum("Mysqlx.Expr.Expr.Type.ARRAY")
        msg["array"] = build_array(value).get_message()
    else:
        msg["type"] = mysqlxpb_enum("Mysqlx.Expr.Expr.Type.LITERAL")
        msg["literal"] = build_scalar(value).get_message()
    return msg


def build_scalar(value: BuildScalarTypes) -> MessageType:
    if isinstance(value, str):
        return build_string_scalar(value)
    if isinstance(value, BYTE_TYPES):
        return build_bytes_scalar(value)
    if isinstance(value, bool):
        return build_bool_scalar(value)
    if isinstance(value, int):
        return build_int_scalar(value)
    if isinstance(value, float):
        return build_double_scalar(value)
    if value is None:
        return build_null_scalar()
    raise ValueError(f"Unsupported data type: {type(value)}")


def build_object(obj: Union[Dict, DbDoc]) -> MessageType:
    if isinstance(obj, DbDoc):
        return build_object(obj.__dict__)

    msg = Message("Mysqlx.Expr.Object")
    for key, value in obj.items():
        pair = Message("Mysqlx.Expr.Object.ObjectField")
        pair["key"] = key.encode() if isinstance(key, str) else key
        pair["value"] = build_expr(value).get_message()
        msg["fld"].extend([pair.get_message()])
    return msg


def build_array(array: Sequence[BuildExprTypes]) -> MessageType:
    msg = Message("Mysqlx.Expr.Array")
    msg["value"].extend([build_expr(value).get_message() for value in array])
    return msg


def build_null_scalar() -> MessageType:
    msg = Message("Mysqlx.Datatypes.Scalar")
    msg["type"] = mysqlxpb_enum("Mysqlx.Datatypes.Scalar.Type.V_NULL")
    return msg


def build_double_scalar(value: float) -> MessageType:
    msg = Message("Mysqlx.Datatypes.Scalar")
    msg["type"] = mysqlxpb_enum("Mysqlx.Datatypes.Scalar.Type.V_DOUBLE")
    msg["v_double"] = value
    return msg


def build_int_scalar(value: int) -> MessageType:
    msg = Message("Mysqlx.Datatypes.Scalar")
    msg["type"] = mysqlxpb_enum("Mysqlx.Datatypes.Scalar.Type.V_SINT")
    msg["v_signed_int"] = value
    return msg


def build_unsigned_int_scalar(value: int) -> MessageType:
    msg = Message("Mysqlx.Datatypes.Scalar")
    msg["type"] = mysqlxpb_enum("Mysqlx.Datatypes.Scalar.Type.V_UINT")
    msg["v_unsigned_int"] = value
    return msg


def build_string_scalar(value: StrOrBytes) -> MessageType:
    if isinstance(value, str):
        value = bytes(bytearray(value, "utf-8"))
    msg = Message("Mysqlx.Datatypes.Scalar")
    msg["type"] = mysqlxpb_enum("Mysqlx.Datatypes.Scalar.Type.V_STRING")
    msg["v_string"] = Message("Mysqlx.Datatypes.Scalar.String", value=value)
    return msg


def build_bool_scalar(value: bool) -> MessageType:
    msg = Message("Mysqlx.Datatypes.Scalar")
    msg["type"] = mysqlxpb_enum("Mysqlx.Datatypes.Scalar.Type.V_BOOL")
    msg["v_bool"] = value
    return msg


def build_bytes_scalar(value: bytes) -> MessageType:
    msg = Message("Mysqlx.Datatypes.Scalar")
    msg["type"] = mysqlxpb_enum("Mysqlx.Datatypes.Scalar.Type.V_OCTETS")
    msg["v_octets"] = Message("Mysqlx.Datatypes.Scalar.Octets", value=value)
    return msg


def build_literal_expr(value: MessageType) -> MessageType:
    msg = Message("Mysqlx.Expr.Expr")
    msg["type"] = mysqlxpb_enum("Mysqlx.Expr.Expr.Type.LITERAL")
    msg["literal"] = value
    return msg


def build_unary_op(name: str, param: MessageType) -> MessageType:
    operator = Message("Mysqlx.Expr.Operator")
    operator["name"] = _UNARY_OPERATORS[name]
    operator["param"] = [param.get_message()]
    msg = Message("Mysqlx.Expr.Expr")
    msg["type"] = mysqlxpb_enum("Mysqlx.Expr.Expr.Type.OPERATOR")
    msg["operator"] = operator.get_message()
    return msg


def escape_literal(string: str) -> str:
    return string.replace('"', '""')


class ExprParser:
    """Expression parser class."""

    def __init__(self, string: str, allow_relational: bool = True) -> None:
        self.string: str = string
        self.tokens: List[Token] = []
        self.path_name_queue: List[str] = []
        self.pos: int = 0
        self._allow_relational_columns: bool = allow_relational
        self.placeholder_name_to_position: Dict[str, int] = {}
        self.positional_placeholder_count: int = 0
        self.clean_expression()
        self.lex()

    def __str__(self) -> str:
        return f"<mysqlx.ExprParser '{self.string}'>"

    def clean_expression(self) -> None:
        """Removes the keywords that does not form part of the expression.

        Removes the keywords "SELECT" and "WHERE" that does not form part of
        the expression itself.
        """
        if not isinstance(self.string, str):
            self.string = repr(self.string)
        self.string = self.string.strip(" ")
        if len(self.string) > 1 and self.string[-1] == ";":
            self.string = self.string[:-1]
        if "SELECT" in self.string[:6].upper():
            self.string = self.string[6:]
        if "WHERE" in self.string[:5].upper():
            self.string = self.string[5:]

    # convencience checker for lexer
    def next_char_is(self, key: int, char: str) -> bool:
        return key + 1 < len(self.string) and self.string[key + 1] == char

    def lex_number(self, pos: int) -> Token:
        # numeric literal
        start = pos
        found_dot = False
        while pos < len(self.string) and (
            self.string[pos].isdigit() or self.string[pos] == "."
        ):
            if self.string[pos] == ".":
                if found_dot is True:
                    raise ValueError("Invalid number. Found multiple '.'")
                found_dot = True
            # technically we allow more than one "." and let float()'s parsing
            # complain later
            pos += 1
        val = self.string[start:pos]
        return Token(TokenType.LNUM, val, len(val))

    def lex_alpha(self, i: int, allow_space: bool = False) -> Token:
        start = i
        while i < len(self.string) and (
            self.string[i].isalnum()
            or self.string[i] == "_"
            or (self.string[i].isspace() and allow_space)
        ):
            i += 1

        val = self.string[start:i]
        try:
            if (
                i < len(self.string)
                and self.string[i] == "("
                and val.lower() not in _SQL_FUNTION_RESERVED_WORDS_COLLISION
            ):
                token = Token(TokenType.IDENT, val, len(val))
            else:
                token = Token(_RESERVED_WORDS[val.lower()], val.lower(), len(val))
        except KeyError:
            token = Token(TokenType.IDENT, val, len(val))
        return token

    def lex_quoted_token(self, key: int) -> Token:
        quote_char = self.string[key]
        val = ""
        key += 1
        start = key
        while key < len(self.string):
            char = self.string[key]
            if (
                char == quote_char
                and key + 1 < len(self.string)
                and self.string[key + 1] != quote_char
            ):
                # break if we have a quote char that's not double
                break
            if char in (quote_char, "\\"):
                # this quote char has to be doubled
                if key + 1 >= len(self.string):
                    break
                key += 1
                val += self.string[key]
            else:
                val += char
            key += 1
        if key >= len(self.string) or self.string[key] != quote_char:
            raise ValueError(f"Unterminated quoted string starting at {start}")
        if quote_char == "`":
            return Token(TokenType.IDENT, val, len(val) + 2)
        return Token(TokenType.LSTRING, val, len(val) + 2)

    def lex(self) -> None:
        i = 0
        arrow_last = False
        inside_arrow = False
        while i < len(self.string):
            char = self.string[i]
            if char.isspace():
                i += 1
                continue
            if char.isdigit():
                token = self.lex_number(i)
            elif char.isalpha() or char == "_":
                token = self.lex_alpha(i, inside_arrow)
            elif char == "?":
                token = Token(TokenType.EROTEME, char)
            elif char == ":":
                token = Token(TokenType.COLON, char)
            elif char == "{":
                token = Token(TokenType.LCURLY, char)
            elif char == "}":
                token = Token(TokenType.RCURLY, char)
            elif char == "+":
                token = Token(TokenType.PLUS, char)
            elif char == "-":
                if self.next_char_is(i, ">") and not arrow_last:
                    token = Token(TokenType.ARROW, "->", 2)
                    arrow_last = True
                else:
                    token = Token(TokenType.MINUS, char)
            elif char == "*":
                if self.next_char_is(i, "*"):
                    token = Token(TokenType.DOUBLESTAR, "**", 2)
                else:
                    token = Token(TokenType.MUL, char)
            elif char == "/":
                token = Token(TokenType.DIV, char)
            elif char == "$":
                token = Token(TokenType.DOLLAR, char)
            elif char == "%":
                token = Token(TokenType.MOD, char)
            elif char == "=":
                if self.next_char_is(i, "="):
                    token = Token(TokenType.EQ, "==", 2)
                else:
                    token = Token(TokenType.EQ, "==", 1)
            elif char == "&":
                if self.next_char_is(i, "&"):
                    token = Token(TokenType.ANDAND, "&&", 2)
                else:
                    token = Token(TokenType.BITAND, char)
            elif char == "^":
                token = Token(TokenType.BITXOR, char)
            elif char == "|":
                if self.next_char_is(i, "|"):
                    token = Token(TokenType.OROR, "||", 2)
                else:
                    token = Token(TokenType.BITOR, char)
            elif char == "(":
                token = Token(TokenType.LPAREN, char)
            elif char == ")":
                token = Token(TokenType.RPAREN, char)
            elif char == "[":
                token = Token(TokenType.LSQBRACKET, char)
            elif char == "]":
                token = Token(TokenType.RSQBRACKET, char)
            elif char == "~":
                token = Token(TokenType.NEG, char)
            elif char == ",":
                token = Token(TokenType.COMMA, char)
            elif char == "!":
                if self.next_char_is(i, "="):
                    token = Token(TokenType.NE, "!=", 2)
                else:
                    token = Token(TokenType.BANG, char)
            elif char == "<":
                if self.next_char_is(i, ">"):
                    token = Token(TokenType.NE, "<>", 2)
                elif self.next_char_is(i, "<"):
                    token = Token(TokenType.LSHIFT, "<<", 2)
                elif self.next_char_is(i, "="):
                    token = Token(TokenType.LE, "<=", 2)
                else:
                    token = Token(TokenType.LT, char)
            elif char == ">":
                if self.next_char_is(i, ">"):
                    token = Token(TokenType.RSHIFT, ">>", 2)
                elif self.next_char_is(i, "="):
                    token = Token(TokenType.GE, ">=", 2)
                else:
                    token = Token(TokenType.GT, char)
            elif char == ".":
                if self.next_char_is(i, "*"):
                    token = Token(TokenType.DOTSTAR, ".*", 2)
                elif i + 1 < len(self.string) and self.string[i + 1].isdigit():
                    token = self.lex_number(i)
                else:
                    token = Token(TokenType.DOT, char)
            elif char in ("'", '"') and arrow_last:
                token = Token(TokenType.QUOTE, char)
                if not inside_arrow:
                    inside_arrow = True
                else:
                    arrow_last = False
                    inside_arrow = False
            elif char in ('"', "'", "`"):
                token = self.lex_quoted_token(i)
            else:
                raise ValueError(f"Unknown character at {i}")
            self.tokens.append(token)
            i += token.length

    def assert_cur_token(self, token_type: int) -> None:
        if self.pos >= len(self.tokens):
            raise ValueError(
                f"Expected token type {token_type} at pos {self.pos} but no "
                "tokens left"
            )
        if self.tokens[self.pos].token_type != token_type:
            raise ValueError(
                f"Expected token type {token_type} at pos {self.pos} but found "
                f"type {self.tokens[self.pos]}, on tokens {self.tokens}"
            )

    def cur_token_type_is(self, token_type: int) -> bool:
        return self.pos_token_type_is(self.pos, token_type)

    def cur_token_type_in(self, *types: int) -> bool:
        return self.pos < len(self.tokens) and self.tokens[self.pos].token_type in types

    def next_token_type_is(self, token_type: int) -> bool:
        return self.pos_token_type_is(self.pos + 1, token_type)

    def next_token_type_in(self, *types: int) -> bool:
        return (
            self.pos < len(self.tokens)
            and self.tokens[self.pos + 1].token_type in types
        )

    def pos_token_type_is(self, pos: int, token_type: int) -> bool:
        return pos < len(self.tokens) and self.tokens[pos].token_type == token_type

    def consume_token(self, token_type: int) -> str:
        self.assert_cur_token(token_type)
        value = self.tokens[self.pos].value
        self.pos += 1
        return value

    def paren_expr_list(
        self,
    ) -> List[Union[ProtobufMessageType, ProtobufMessageCextType]]:
        """Parse a paren-bounded expression list for function arguments or IN
        list and return a list of Expr objects.
        """
        exprs = []
        path_name_added = False
        self.consume_token(TokenType.LPAREN)
        if not self.cur_token_type_is(TokenType.RPAREN):
            msg_expr = self._expr().get_message()
            if hasattr(msg_expr, "identifier") and msg_expr.identifier.name:
                self.path_name_queue.insert(0, msg_expr.identifier.name)
                path_name_added = True
            elif (
                not hasattr(msg_expr, "identifier")
                and "identifier" in msg_expr
                and "name" in msg_expr["identifier"]
            ):
                self.path_name_queue.insert(0, msg_expr["identifier"]["name"])
                path_name_added = True
            exprs.append(msg_expr)
            while self.cur_token_type_is(TokenType.COMMA):
                self.pos += 1
                exprs.append(self._expr().get_message())
        self.consume_token(TokenType.RPAREN)
        if path_name_added:
            self.path_name_queue.pop()
        return exprs

    def identifier(self) -> MessageType:
        self.assert_cur_token(TokenType.IDENT)
        ident = Message("Mysqlx.Expr.Identifier")
        if self.next_token_type_is(TokenType.DOT):
            ident["schema_name"] = self.consume_token(TokenType.IDENT)
            self.consume_token(TokenType.DOT)
        ident["name"] = self.tokens[self.pos].value
        self.pos += 1
        return ident

    def function_call(self) -> MessageType:
        function_call = Message("Mysqlx.Expr.FunctionCall")
        function_call["name"] = self.identifier()
        function_call["param"] = self.paren_expr_list()
        msg_expr = Message("Mysqlx.Expr.Expr")
        msg_expr["type"] = mysqlxpb_enum("Mysqlx.Expr.Expr.Type.FUNC_CALL")
        msg_expr["function_call"] = function_call.get_message()
        return msg_expr

    def docpath_member(self) -> MessageType:
        self.consume_token(TokenType.DOT)
        token = self.tokens[self.pos]

        if token.token_type == TokenType.IDENT:
            if token.value.startswith("`") and token.value.endswith("`"):
                raise ValueError(
                    f"{token.value} is not a valid JSON/ECMAScript identifier"
                )
            self.consume_token(TokenType.IDENT)
            member_name = token.value
        elif token.token_type == TokenType.LSTRING:
            self.consume_token(TokenType.LSTRING)
            member_name = token.value
        else:
            raise ValueError(
                "Expected token type IDENT or LSTRING in JSON path at token "
                f"pos {self.pos}"
            )
        doc_path_item = Message("Mysqlx.Expr.DocumentPathItem")
        doc_path_item["type"] = mysqlxpb_enum(
            "Mysqlx.Expr.DocumentPathItem.Type.MEMBER"
        )
        doc_path_item["value"] = member_name
        return doc_path_item

    def docpath_array_loc(self) -> MessageType:
        self.consume_token(TokenType.LSQBRACKET)
        if self.cur_token_type_is(TokenType.MUL):
            self.consume_token(TokenType.MUL)
            self.consume_token(TokenType.RSQBRACKET)
            doc_path_item = Message("Mysqlx.Expr.DocumentPathItem")
            doc_path_item["type"] = mysqlxpb_enum(
                "Mysqlx.Expr.DocumentPathItem.Type.ARRAY_INDEX_ASTERISK"
            )
            return doc_path_item
        if self.cur_token_type_is(TokenType.LNUM):
            value = int(self.consume_token(TokenType.LNUM))
            if value < 0:
                raise IndexError(f"Array index cannot be negative at {self.pos}")
            self.consume_token(TokenType.RSQBRACKET)
            doc_path_item = Message("Mysqlx.Expr.DocumentPathItem")
            doc_path_item["type"] = mysqlxpb_enum(
                "Mysqlx.Expr.DocumentPathItem.Type.ARRAY_INDEX"
            )
            doc_path_item["index"] = value
            return doc_path_item
        raise ValueError(
            "Exception token type MUL or LNUM in JSON path array index at "
            f"token pos {self.pos}"
        )

    def document_field(self) -> MessageType:
        if not self.tokens:
            raise ValueError("Empty string cannot be used as document field")
        if self.cur_token_type_is(TokenType.DOLLAR):
            self.consume_token(TokenType.DOLLAR)
        col_id = Message("Mysqlx.Expr.ColumnIdentifier")
        if self.cur_token_type_is(TokenType.IDENT):
            doc_path_item = Message("Mysqlx.Expr.DocumentPathItem")
            doc_path_item["type"] = mysqlxpb_enum(
                "Mysqlx.Expr.DocumentPathItem.Type.MEMBER"
            )
            doc_path_item["value"] = self.consume_token(TokenType.IDENT)
            col_id["document_path"].extend([doc_path_item.get_message()])
        col_id["document_path"].extend(self.document_path())
        if self.path_name_queue:
            col_id["name"] = self.path_name_queue[0]
        expr = Message("Mysqlx.Expr.Expr")
        expr["type"] = mysqlxpb_enum("Mysqlx.Expr.Expr.Type.IDENT")
        expr["identifier"] = col_id
        return expr

    def document_path(
        self,
    ) -> List[Union[ProtobufMessageType, ProtobufMessageCextType]]:
        """Parse a JSON-style document path, like WL#7909, but prefix by @.
        instead of $. We parse this as a string because the protocol doesn't
        support it. (yet)
        """
        doc_path = []
        while True:
            if self.cur_token_type_is(TokenType.DOT):
                doc_path.append(self.docpath_member().get_message())
            elif self.cur_token_type_is(TokenType.DOTSTAR):
                self.consume_token(TokenType.DOTSTAR)
                doc_path_item = Message("Mysqlx.Expr.DocumentPathItem")
                doc_path_item["type"] = mysqlxpb_enum(
                    "Mysqlx.Expr.DocumentPathItem.Type.MEMBER_ASTERISK"
                )
                doc_path.append(doc_path_item.get_message())
            elif self.cur_token_type_is(TokenType.LSQBRACKET):
                doc_path.append(self.docpath_array_loc().get_message())
            elif self.cur_token_type_is(TokenType.DOUBLESTAR):
                self.consume_token(TokenType.DOUBLESTAR)
                doc_path_item = Message("Mysqlx.Expr.DocumentPathItem")
                doc_path_item["type"] = mysqlxpb_enum(
                    "Mysqlx.Expr.DocumentPathItem.Type.DOUBLE_ASTERISK"
                )
                doc_path.append(doc_path_item.get_message())
            else:
                break
        items = len(doc_path)
        if items > 0 and get_item_or_attr(doc_path[items - 1], "type") == mysqlxpb_enum(
            "Mysqlx.Expr.DocumentPathItem.Type.DOUBLE_ASTERISK"
        ):
            raise ValueError(f"JSON path may not end in '**' at {self.pos}")
        return doc_path

    def column_identifier(self) -> MessageType:
        parts = []
        parts.append(self.consume_token(TokenType.IDENT))
        while self.cur_token_type_is(TokenType.DOT):
            self.consume_token(TokenType.DOT)
            parts.append(self.consume_token(TokenType.IDENT))
        if len(parts) > 3:
            raise ValueError(f"Too many parts to identifier at {self.pos}")
        parts.reverse()
        col_id = Message("Mysqlx.Expr.ColumnIdentifier")
        # clever way to apply them to the struct
        for i in range(0, len(parts)):
            if i == 0:
                col_id["name"] = parts[0]
            elif i == 1:
                col_id["table_name"] = parts[1]
            elif i == 2:
                col_id["schema_name"] = parts[2]

        is_doc = False
        if self.cur_token_type_is(TokenType.DOLLAR):
            is_doc = True
            self.consume_token(TokenType.DOLLAR)
            col_id["document_path"] = self.document_path()
        elif self.cur_token_type_is(TokenType.ARROW):
            is_doc = True
            self.consume_token(TokenType.ARROW)
            is_quoted = False
            if self.cur_token_type_is(TokenType.QUOTE):
                is_quoted = True
                self.consume_token(TokenType.QUOTE)
            self.consume_token(TokenType.DOLLAR)
            col_id["document_path"] = self.document_path()
            if is_quoted:
                self.consume_token(TokenType.QUOTE)

        if is_doc and len(col_id["document_path"]) == 0:
            doc_path_item = Message("Mysqlx.Expr.DocumentPathItem")
            doc_path_item["type"] = mysqlxpb_enum(
                "Mysqlx.Expr.DocumentPathItem.Type.MEMBER"
            )
            doc_path_item["value"] = ""
            col_id["document_path"].extend([doc_path_item.get_message()])

        msg_expr = Message("Mysqlx.Expr.Expr")
        msg_expr["type"] = mysqlxpb_enum("Mysqlx.Expr.Expr.Type.IDENT")
        msg_expr["identifier"] = col_id
        return msg_expr

    def next_token(self) -> Token:
        if self.pos >= len(self.tokens):
            raise ValueError("Unexpected end of token stream")
        token = self.tokens[self.pos]
        self.pos += 1
        return token

    def expect_token(self, token_type: int) -> None:
        token = self.next_token()
        if token.token_type != token_type:
            raise ValueError(f"Expected token type {token_type}")

    def peek_token(self) -> Token:
        return self.tokens[self.pos]

    def consume_any_token(self) -> str:
        value = self.tokens[self.pos].value
        self.pos += 1
        return value

    def parse_json_array(self) -> MessageType:
        """
        jsonArray            ::=  "[" [ expr ("," expr)* ] "]"
        """
        msg = Message("Mysqlx.Expr.Array")
        while self.pos < len(self.tokens) and not self.cur_token_type_is(
            TokenType.RSQBRACKET
        ):
            msg["value"].extend([self._expr().get_message()])
            if not self.cur_token_type_is(TokenType.COMMA):
                break
            self.consume_token(TokenType.COMMA)
        self.consume_token(TokenType.RSQBRACKET)

        expr = Message("Mysqlx.Expr.Expr")
        expr["type"] = mysqlxpb_enum("Mysqlx.Expr.Expr.Type.ARRAY")
        expr["array"] = msg.get_message()
        return expr

    def parse_json_doc(self) -> MessageType:
        """
        jsonDoc              ::=  "{" [jsonKeyValue ("," jsonKeyValue)*] "}"
        jsonKeyValue         ::=  STRING_DQ ":" expr
        """
        msg = Message("Mysqlx.Expr.Object")
        while self.pos < len(self.tokens) and not self.cur_token_type_is(
            TokenType.RCURLY
        ):
            item = Message("Mysqlx.Expr.Object.ObjectField")
            item["key"] = self.consume_token(TokenType.LSTRING)
            self.consume_token(TokenType.COLON)
            item["value"] = self._expr().get_message()
            msg["fld"].extend([item.get_message()])
            if not self.cur_token_type_is(TokenType.COMMA):
                break
            self.consume_token(TokenType.COMMA)
        self.consume_token(TokenType.RCURLY)

        expr = Message("Mysqlx.Expr.Expr")
        expr["type"] = mysqlxpb_enum("Mysqlx.Expr.Expr.Type.OBJECT")
        expr["object"] = msg.get_message()
        return expr

    def parse_place_holder(self, token: Token) -> MessageType:
        place_holder_name = ""
        if self.cur_token_type_is(TokenType.LNUM):
            place_holder_name = self.consume_token(TokenType.LNUM)
        elif self.cur_token_type_is(TokenType.IDENT):
            place_holder_name = self.consume_token(TokenType.IDENT)
        elif token.token_type == TokenType.EROTEME:
            place_holder_name = str(self.positional_placeholder_count)
        else:
            raise ValueError(f"Invalid placeholder name at token pos {self.pos}")

        msg_expr = Message("Mysqlx.Expr.Expr")
        msg_expr["type"] = mysqlxpb_enum("Mysqlx.Expr.Expr.Type.PLACEHOLDER")
        if place_holder_name in self.placeholder_name_to_position:
            msg_expr["position"] = self.placeholder_name_to_position[place_holder_name]
        else:
            msg_expr["position"] = self.positional_placeholder_count
            self.placeholder_name_to_position[
                place_holder_name
            ] = self.positional_placeholder_count
            self.positional_placeholder_count += 1
        return msg_expr

    def cast(self) -> MessageType:
        """cast ::= CAST LPAREN expr AS cast_data_type RPAREN"""
        operator = Message("Mysqlx.Expr.Operator", name="cast")
        self.consume_token(TokenType.LPAREN)
        operator["param"].extend([self._expr().get_message()])
        self.consume_token(TokenType.AS)

        type_scalar = build_bytes_scalar(str.encode(self.cast_data_type()))
        operator["param"].extend([build_literal_expr(type_scalar).get_message()])
        self.consume_token(TokenType.RPAREN)
        msg = Message("Mysqlx.Expr.Expr", operator=operator.get_message())
        msg["type"] = mysqlxpb_enum("Mysqlx.Expr.Expr.Type.OPERATOR")
        return msg

    def cast_data_type(self) -> str:
        """cast_data_type ::= ( BINARY dimension? ) |
        ( CHAR dimension? ) |
        ( DATE ) |
        ( DATETIME dimension? ) |
        ( TIME dimension? ) |
        ( DECIMAL dimension? ) |
        ( SIGNED INTEGER? ) |
        ( UNSIGNED INTEGER? ) |
        JSON
        """
        token = self.next_token()
        if token.token_type in (
            TokenType.BINARY,
            TokenType.CHAR,
            TokenType.DATETIME,
            TokenType.TIME,
        ):
            dimension = self.cast_data_type_dimension()
            return f"{token.value}{dimension}" if dimension else token.value
        if token.token_type is TokenType.DECIMAL:
            dimension = self.cast_data_type_dimension(True)
            return f"{token.value}{dimension}" if dimension else token.value
        if token.token_type in (TokenType.SIGNED, TokenType.UNSIGNED):
            if self.cur_token_type_is(TokenType.INTEGER):
                self.consume_token(TokenType.INTEGER)
            return token.value
        if token.token_type in (
            TokenType.INTEGER,
            TokenType.JSON,
            TokenType.DATE,
        ):
            return token.value

        raise ValueError(
            f"Unknown token type {token.token_type} at position {self.pos} "
            f"({token.value})"
        )

    def cast_data_type_dimension(self, decimal: bool = False) -> Optional[str]:
        """dimension ::= LPAREN LNUM (, LNUM)? RPAREN"""
        if not self.cur_token_type_is(TokenType.LPAREN):
            return None

        dimension = []
        self.consume_token(TokenType.LPAREN)
        dimension.append(self.consume_token(TokenType.LNUM))
        if decimal and self.cur_token_type_is(TokenType.COMMA):
            self.consume_token(TokenType.COMMA)
            dimension.append(self.consume_token(TokenType.LNUM))
        self.consume_token(TokenType.RPAREN)

        return (
            f"({dimension[0]})"
            if len(dimension) == 1
            else f"({dimension[0]},{dimension[1]})"
        )

    @staticmethod
    def star_operator() -> MessageType:
        msg = Message("Mysqlx.Expr.Expr")
        msg["type"] = mysqlxpb_enum("Mysqlx.Expr.Expr.Type.OPERATOR")
        msg["operator"] = Message("Mysqlx.Expr.Operator", name="*")
        return msg

    def atomic_expr(self) -> MessageType:
        """Parse an atomic expression and return a protobuf Expr object"""
        token = self.next_token()

        if token.token_type in [TokenType.EROTEME, TokenType.COLON]:
            return self.parse_place_holder(token)
        if token.token_type == TokenType.LCURLY:
            return self.parse_json_doc()
        if token.token_type == TokenType.LSQBRACKET:
            return self.parse_json_array()
        if token.token_type == TokenType.CAST:
            return self.cast()
        if token.token_type == TokenType.LPAREN:
            expr = self._expr()
            self.expect_token(TokenType.RPAREN)
            return expr
        if token.token_type in [TokenType.PLUS, TokenType.MINUS]:
            peek = self.peek_token()
            if peek.token_type == TokenType.LNUM:
                self.tokens[self.pos].value = token.value + peek.value
                return self.atomic_expr()
            return build_unary_op(token.value, self.atomic_expr())
        if token.token_type in (TokenType.NOT, TokenType.NEG, TokenType.BANG):
            return build_unary_op(token.value, self.atomic_expr())
        if token.token_type == TokenType.LSTRING:
            return build_literal_expr(build_string_scalar(token.value))
        if token.token_type == TokenType.NULL:
            return build_literal_expr(build_null_scalar())
        if token.token_type == TokenType.LNUM:
            if "." in token.value:
                return build_literal_expr(build_double_scalar(float(token.value)))
            return build_literal_expr(build_int_scalar(int(token.value)))
        if token.token_type in [TokenType.TRUE, TokenType.FALSE]:
            return build_literal_expr(
                build_bool_scalar(token.token_type == TokenType.TRUE)
            )
        if token.token_type == TokenType.DOLLAR:
            return self.document_field()
        if token.token_type == TokenType.MUL:
            return self.star_operator()
        if token.token_type == TokenType.IDENT:
            self.pos = self.pos - 1  # stay on the identifier
            if self.next_token_type_is(TokenType.LPAREN) or (
                self.next_token_type_is(TokenType.DOT)
                and self.pos_token_type_is(self.pos + 2, TokenType.IDENT)
                and self.pos_token_type_is(self.pos + 3, TokenType.LPAREN)
            ):
                # Function call
                return self.function_call()
            return (
                self.document_field()
                if not self._allow_relational_columns
                else self.column_identifier()
            )

        raise ValueError(
            f"Unknown token type = {token.token_type}  when expecting atomic "
            f"expression at {self.pos}"
        )

    def parse_left_assoc_binary_op_expr(
        self, types: Iterable[int], inner_parser: Callable[[], MessageType]
    ) -> MessageType:
        """Given a `set' of types and an Expr-returning inner parser function,
        parse a left associate binary operator expression"""
        lhs = inner_parser()
        while self.pos < len(self.tokens) and self.tokens[self.pos].token_type in types:
            msg = Message("Mysqlx.Expr.Expr")
            msg["type"] = mysqlxpb_enum("Mysqlx.Expr.Expr.Type.OPERATOR")
            operator = Message("Mysqlx.Expr.Operator")
            operator["name"] = _OPERATORS[self.tokens[self.pos].value]
            operator["param"] = [lhs.get_message()]
            self.pos += 1
            operator["param"].extend([inner_parser().get_message()])
            msg["operator"] = operator
            lhs = msg
        return lhs

    # operator precedence is implemented here
    def add_sub_interval(self) -> MessageType:
        lhs = self.atomic_expr()
        if self.cur_token_type_in(
            TokenType.PLUS, TokenType.MINUS
        ) and self.next_token_type_is(TokenType.INTERVAL):
            token = self.next_token()

            operator = Message("Mysqlx.Expr.Operator")
            operator["param"].extend([lhs.get_message()])
            operator["name"] = (
                "date_add" if token.token_type is TokenType.PLUS else "date_sub"
            )

            self.consume_token(TokenType.INTERVAL)
            operator["param"].extend([self.bit_expr().get_message()])

            if not self.cur_token_type_in(*_INTERVAL_UNITS):
                raise ValueError(f"Expected interval type at position {self.pos}")

            token = str.encode(self.consume_any_token().upper())
            operator["param"].extend(
                [build_literal_expr(build_bytes_scalar(token)).get_message()]
            )

            lhs = Message("Mysqlx.Expr.Expr", operator=operator)
            lhs["type"] = mysqlxpb_enum("Mysqlx.Expr.Expr.Type.OPERATOR")

        return lhs

    def mul_div_expr(self) -> MessageType:
        return self.parse_left_assoc_binary_op_expr(
            set([TokenType.MUL, TokenType.DIV, TokenType.MOD]),
            self.add_sub_interval,
        )

    def add_sub_expr(self) -> MessageType:
        return self.parse_left_assoc_binary_op_expr(
            set([TokenType.PLUS, TokenType.MINUS]), self.mul_div_expr
        )

    def shift_expr(self) -> MessageType:
        return self.parse_left_assoc_binary_op_expr(
            set([TokenType.LSHIFT, TokenType.RSHIFT]), self.add_sub_expr
        )

    def bit_expr(self) -> MessageType:
        return self.parse_left_assoc_binary_op_expr(
            set([TokenType.BITAND, TokenType.BITOR, TokenType.BITXOR]),
            self.shift_expr,
        )

    def comp_expr(self) -> MessageType:
        return self.parse_left_assoc_binary_op_expr(
            set(
                [
                    TokenType.GE,
                    TokenType.GT,
                    TokenType.LE,
                    TokenType.LT,
                    TokenType.EQ,
                    TokenType.NE,
                ]
            ),
            self.bit_expr,
        )

    def ilri_expr(self) -> MessageType:
        params = []
        lhs = self.comp_expr()
        is_not = False
        if self.cur_token_type_is(TokenType.NOT):
            is_not = True
            self.consume_token(TokenType.NOT)
        if self.pos < len(self.tokens):
            params.append(lhs.get_message())
            op_name = self.tokens[self.pos].value
            if self.cur_token_type_is(TokenType.IS):
                self.consume_token(TokenType.IS)
                # for IS, NOT comes AFTER
                if self.cur_token_type_is(TokenType.NOT):
                    is_not = True
                    self.consume_token(TokenType.NOT)
                params.append(self.comp_expr().get_message())
            elif self.cur_token_type_is(TokenType.IN):
                self.consume_token(TokenType.IN)
                if self.cur_token_type_is(TokenType.LPAREN):
                    params.extend(self.paren_expr_list())
                else:
                    op_name = "cont_in"
                    params.append(self.comp_expr().get_message())
            elif self.cur_token_type_is(TokenType.OVERLAPS):
                self.consume_token(TokenType.OVERLAPS)
                params.append(self.comp_expr().get_message())

            elif self.cur_token_type_is(TokenType.LIKE):
                self.consume_token(TokenType.LIKE)
                params.append(self.comp_expr().get_message())
                if self.cur_token_type_is(TokenType.ESCAPE):
                    self.consume_token(TokenType.ESCAPE)
                    params.append(self.comp_expr().get_message())
            elif self.cur_token_type_is(TokenType.BETWEEN):
                self.consume_token(TokenType.BETWEEN)
                params.append(self.comp_expr().get_message())
                self.consume_token(TokenType.AND)
                params.append(self.comp_expr().get_message())
            elif self.cur_token_type_is(TokenType.REGEXP):
                self.consume_token(TokenType.REGEXP)
                params.append(self.comp_expr().get_message())
            else:
                if is_not:
                    raise ValueError(f"Unknown token after NOT as pos {self.pos}")
                op_name = None  # not an operator we're interested in
            if op_name:
                operator = Message("Mysqlx.Expr.Operator")
                operator["name"] = _NEGATION[op_name] if is_not else op_name
                operator["param"] = params
                msg_expr = Message("Mysqlx.Expr.Expr")
                msg_expr["type"] = mysqlxpb_enum("Mysqlx.Expr.Expr.Type.OPERATOR")
                msg_expr["operator"] = operator.get_message()
                lhs = msg_expr
        return lhs

    def and_expr(self) -> MessageType:
        return self.parse_left_assoc_binary_op_expr(
            set([TokenType.AND, TokenType.ANDAND]), self.ilri_expr
        )

    def xor_expr(self) -> MessageType:
        return self.parse_left_assoc_binary_op_expr(set([TokenType.XOR]), self.and_expr)

    def or_expr(self) -> MessageType:
        return self.parse_left_assoc_binary_op_expr(
            set([TokenType.OR, TokenType.OROR]), self.xor_expr
        )

    def _expr(self, reparse: bool = False) -> MessageType:
        if reparse:
            self.tokens = []
            self.pos = 0
            self.placeholder_name_to_position = {}
            self.positional_placeholder_count = 0
            self.lex()
        return self.or_expr()

    def expr(self, reparse: bool = False) -> MessageType:
        expression = self._expr(reparse)
        used_tokens = self.pos
        if self.pos_token_type_is(len(self.tokens) - 2, TokenType.AS):
            used_tokens += 2
        if used_tokens < len(self.tokens):
            raise ValueError(
                f"Unused token types {self.tokens[self.pos :]} found in "
                f"expression at position: {self.pos}"
            )
        return expression

    def parse_table_insert_field(self) -> MessageType:
        return Message("Mysqlx.Crud.Column", name=self.consume_token(TokenType.IDENT))

    def parse_table_update_field(self) -> Any:
        return self.column_identifier().identifier

    def _table_fields(self) -> List[str]:
        fields = []
        temp = self.string.split(",")
        temp.reverse()
        while temp:
            field = temp.pop()
            while (
                field.count("(") != field.count(")")
                or field.count("[") != field.count("]")
                or field.count("{") != field.count("}")
            ):
                field = f"{temp.pop()},{field}"
            fields.append(field.strip())
        return fields

    def parse_table_select_projection(
        self,
    ) -> List[Union[ProtobufMessageType, ProtobufMessageCextType]]:
        project_expr: List = []
        first = True
        fields = self._table_fields()
        while self.pos < len(self.tokens):
            if not first:
                self.consume_token(TokenType.COMMA)
            first = False
            projection = Message("Mysqlx.Crud.Projection", source=self._expr())
            if self.cur_token_type_is(TokenType.AS):
                self.consume_token(TokenType.AS)
                projection["alias"] = self.consume_token(TokenType.IDENT)
            else:
                projection["alias"] = fields[len(project_expr)]
            project_expr.append(projection.get_message())

        return project_expr

    def parse_order_spec(
        self,
    ) -> List[Union[ProtobufMessageType, ProtobufMessageCextType]]:
        order_specs = []
        first = True
        while self.pos < len(self.tokens):
            if not first:
                self.consume_token(TokenType.COMMA)
            first = False
            order = Message("Mysqlx.Crud.Order", expr=self._expr())
            if self.cur_token_type_is(TokenType.ORDERBY_ASC):
                order["direction"] = mysqlxpb_enum("Mysqlx.Crud.Order.Direction.ASC")
                self.consume_token(TokenType.ORDERBY_ASC)
            elif self.cur_token_type_is(TokenType.ORDERBY_DESC):
                order["direction"] = mysqlxpb_enum("Mysqlx.Crud.Order.Direction.DESC")
                self.consume_token(TokenType.ORDERBY_DESC)
            order_specs.append(order.get_message())
        return order_specs

    def parse_expr_list(
        self,
    ) -> List[Union[ProtobufMessageType, ProtobufMessageCextType]]:
        expr_list = []
        first = True
        while self.pos < len(self.tokens):
            if not first:
                self.consume_token(TokenType.COMMA)
            first = False
            expr_list.append(self._expr().get_message())
        return expr_list
