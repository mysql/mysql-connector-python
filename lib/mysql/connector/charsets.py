# -*- coding: utf-8 -*-

# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2013, 2015, Oracle and/or its affiliates. All rights reserved.

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

# This file was auto-generated.
_GENERATED_ON = '2015-08-24'
_MYSQL_VERSION = (5, 7, 8)

"""This module contains the MySQL Server Character Sets"""

MYSQL_CHARACTER_SETS = [
    # (character set name, collation, default)
    None,
    ("big5", "big5_chinese_ci", True),  # 1
    ("latin2", "latin2_czech_cs", False),  # 2
    ("dec8", "dec8_swedish_ci", True),  # 3
    ("cp850", "cp850_general_ci", True),  # 4
    ("latin1", "latin1_german1_ci", False),  # 5
    ("hp8", "hp8_english_ci", True),  # 6
    ("koi8r", "koi8r_general_ci", True),  # 7
    ("latin1", "latin1_swedish_ci", True),  # 8
    ("latin2", "latin2_general_ci", True),  # 9
    ("swe7", "swe7_swedish_ci", True),  # 10
    ("ascii", "ascii_general_ci", True),  # 11
    ("ujis", "ujis_japanese_ci", True),  # 12
    ("sjis", "sjis_japanese_ci", True),  # 13
    ("cp1251", "cp1251_bulgarian_ci", False),  # 14
    ("latin1", "latin1_danish_ci", False),  # 15
    ("hebrew", "hebrew_general_ci", True),  # 16
    None,
    ("tis620", "tis620_thai_ci", True),  # 18
    ("euckr", "euckr_korean_ci", True),  # 19
    ("latin7", "latin7_estonian_cs", False),  # 20
    ("latin2", "latin2_hungarian_ci", False),  # 21
    ("koi8u", "koi8u_general_ci", True),  # 22
    ("cp1251", "cp1251_ukrainian_ci", False),  # 23
    ("gb2312", "gb2312_chinese_ci", True),  # 24
    ("greek", "greek_general_ci", True),  # 25
    ("cp1250", "cp1250_general_ci", True),  # 26
    ("latin2", "latin2_croatian_ci", False),  # 27
    ("gbk", "gbk_chinese_ci", True),  # 28
    ("cp1257", "cp1257_lithuanian_ci", False),  # 29
    ("latin5", "latin5_turkish_ci", True),  # 30
    ("latin1", "latin1_german2_ci", False),  # 31
    ("armscii8", "armscii8_general_ci", True),  # 32
    ("utf8", "utf8_general_ci", True),  # 33
    ("cp1250", "cp1250_czech_cs", False),  # 34
    ("ucs2", "ucs2_general_ci", True),  # 35
    ("cp866", "cp866_general_ci", True),  # 36
    ("keybcs2", "keybcs2_general_ci", True),  # 37
    ("macce", "macce_general_ci", True),  # 38
    ("macroman", "macroman_general_ci", True),  # 39
    ("cp852", "cp852_general_ci", True),  # 40
    ("latin7", "latin7_general_ci", True),  # 41
    ("latin7", "latin7_general_cs", False),  # 42
    ("macce", "macce_bin", False),  # 43
    ("cp1250", "cp1250_croatian_ci", False),  # 44
    ("utf8mb4", "utf8mb4_general_ci", True),  # 45
    ("utf8mb4", "utf8mb4_bin", False),  # 46
    ("latin1", "latin1_bin", False),  # 47
    ("latin1", "latin1_general_ci", False),  # 48
    ("latin1", "latin1_general_cs", False),  # 49
    ("cp1251", "cp1251_bin", False),  # 50
    ("cp1251", "cp1251_general_ci", True),  # 51
    ("cp1251", "cp1251_general_cs", False),  # 52
    ("macroman", "macroman_bin", False),  # 53
    ("utf16", "utf16_general_ci", True),  # 54
    ("utf16", "utf16_bin", False),  # 55
    ("utf16le", "utf16le_general_ci", True),  # 56
    ("cp1256", "cp1256_general_ci", True),  # 57
    ("cp1257", "cp1257_bin", False),  # 58
    ("cp1257", "cp1257_general_ci", True),  # 59
    ("utf32", "utf32_general_ci", True),  # 60
    ("utf32", "utf32_bin", False),  # 61
    ("utf16le", "utf16le_bin", False),  # 62
    ("binary", "binary", True),  # 63
    ("armscii8", "armscii8_bin", False),  # 64
    ("ascii", "ascii_bin", False),  # 65
    ("cp1250", "cp1250_bin", False),  # 66
    ("cp1256", "cp1256_bin", False),  # 67
    ("cp866", "cp866_bin", False),  # 68
    ("dec8", "dec8_bin", False),  # 69
    ("greek", "greek_bin", False),  # 70
    ("hebrew", "hebrew_bin", False),  # 71
    ("hp8", "hp8_bin", False),  # 72
    ("keybcs2", "keybcs2_bin", False),  # 73
    ("koi8r", "koi8r_bin", False),  # 74
    ("koi8u", "koi8u_bin", False),  # 75
    None,
    ("latin2", "latin2_bin", False),  # 77
    ("latin5", "latin5_bin", False),  # 78
    ("latin7", "latin7_bin", False),  # 79
    ("cp850", "cp850_bin", False),  # 80
    ("cp852", "cp852_bin", False),  # 81
    ("swe7", "swe7_bin", False),  # 82
    ("utf8", "utf8_bin", False),  # 83
    ("big5", "big5_bin", False),  # 84
    ("euckr", "euckr_bin", False),  # 85
    ("gb2312", "gb2312_bin", False),  # 86
    ("gbk", "gbk_bin", False),  # 87
    ("sjis", "sjis_bin", False),  # 88
    ("tis620", "tis620_bin", False),  # 89
    ("ucs2", "ucs2_bin", False),  # 90
    ("ujis", "ujis_bin", False),  # 91
    ("geostd8", "geostd8_general_ci", True),  # 92
    ("geostd8", "geostd8_bin", False),  # 93
    ("latin1", "latin1_spanish_ci", False),  # 94
    ("cp932", "cp932_japanese_ci", True),  # 95
    ("cp932", "cp932_bin", False),  # 96
    ("eucjpms", "eucjpms_japanese_ci", True),  # 97
    ("eucjpms", "eucjpms_bin", False),  # 98
    ("cp1250", "cp1250_polish_ci", False),  # 99
    None,
    ("utf16", "utf16_unicode_ci", False),  # 101
    ("utf16", "utf16_icelandic_ci", False),  # 102
    ("utf16", "utf16_latvian_ci", False),  # 103
    ("utf16", "utf16_romanian_ci", False),  # 104
    ("utf16", "utf16_slovenian_ci", False),  # 105
    ("utf16", "utf16_polish_ci", False),  # 106
    ("utf16", "utf16_estonian_ci", False),  # 107
    ("utf16", "utf16_spanish_ci", False),  # 108
    ("utf16", "utf16_swedish_ci", False),  # 109
    ("utf16", "utf16_turkish_ci", False),  # 110
    ("utf16", "utf16_czech_ci", False),  # 111
    ("utf16", "utf16_danish_ci", False),  # 112
    ("utf16", "utf16_lithuanian_ci", False),  # 113
    ("utf16", "utf16_slovak_ci", False),  # 114
    ("utf16", "utf16_spanish2_ci", False),  # 115
    ("utf16", "utf16_roman_ci", False),  # 116
    ("utf16", "utf16_persian_ci", False),  # 117
    ("utf16", "utf16_esperanto_ci", False),  # 118
    ("utf16", "utf16_hungarian_ci", False),  # 119
    ("utf16", "utf16_sinhala_ci", False),  # 120
    ("utf16", "utf16_german2_ci", False),  # 121
    ("utf16", "utf16_croatian_ci", False),  # 122
    ("utf16", "utf16_unicode_520_ci", False),  # 123
    ("utf16", "utf16_vietnamese_ci", False),  # 124
    None,
    None,
    None,
    ("ucs2", "ucs2_unicode_ci", False),  # 128
    ("ucs2", "ucs2_icelandic_ci", False),  # 129
    ("ucs2", "ucs2_latvian_ci", False),  # 130
    ("ucs2", "ucs2_romanian_ci", False),  # 131
    ("ucs2", "ucs2_slovenian_ci", False),  # 132
    ("ucs2", "ucs2_polish_ci", False),  # 133
    ("ucs2", "ucs2_estonian_ci", False),  # 134
    ("ucs2", "ucs2_spanish_ci", False),  # 135
    ("ucs2", "ucs2_swedish_ci", False),  # 136
    ("ucs2", "ucs2_turkish_ci", False),  # 137
    ("ucs2", "ucs2_czech_ci", False),  # 138
    ("ucs2", "ucs2_danish_ci", False),  # 139
    ("ucs2", "ucs2_lithuanian_ci", False),  # 140
    ("ucs2", "ucs2_slovak_ci", False),  # 141
    ("ucs2", "ucs2_spanish2_ci", False),  # 142
    ("ucs2", "ucs2_roman_ci", False),  # 143
    ("ucs2", "ucs2_persian_ci", False),  # 144
    ("ucs2", "ucs2_esperanto_ci", False),  # 145
    ("ucs2", "ucs2_hungarian_ci", False),  # 146
    ("ucs2", "ucs2_sinhala_ci", False),  # 147
    ("ucs2", "ucs2_german2_ci", False),  # 148
    ("ucs2", "ucs2_croatian_ci", False),  # 149
    ("ucs2", "ucs2_unicode_520_ci", False),  # 150
    ("ucs2", "ucs2_vietnamese_ci", False),  # 151
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    ("ucs2", "ucs2_general_mysql500_ci", False),  # 159
    ("utf32", "utf32_unicode_ci", False),  # 160
    ("utf32", "utf32_icelandic_ci", False),  # 161
    ("utf32", "utf32_latvian_ci", False),  # 162
    ("utf32", "utf32_romanian_ci", False),  # 163
    ("utf32", "utf32_slovenian_ci", False),  # 164
    ("utf32", "utf32_polish_ci", False),  # 165
    ("utf32", "utf32_estonian_ci", False),  # 166
    ("utf32", "utf32_spanish_ci", False),  # 167
    ("utf32", "utf32_swedish_ci", False),  # 168
    ("utf32", "utf32_turkish_ci", False),  # 169
    ("utf32", "utf32_czech_ci", False),  # 170
    ("utf32", "utf32_danish_ci", False),  # 171
    ("utf32", "utf32_lithuanian_ci", False),  # 172
    ("utf32", "utf32_slovak_ci", False),  # 173
    ("utf32", "utf32_spanish2_ci", False),  # 174
    ("utf32", "utf32_roman_ci", False),  # 175
    ("utf32", "utf32_persian_ci", False),  # 176
    ("utf32", "utf32_esperanto_ci", False),  # 177
    ("utf32", "utf32_hungarian_ci", False),  # 178
    ("utf32", "utf32_sinhala_ci", False),  # 179
    ("utf32", "utf32_german2_ci", False),  # 180
    ("utf32", "utf32_croatian_ci", False),  # 181
    ("utf32", "utf32_unicode_520_ci", False),  # 182
    ("utf32", "utf32_vietnamese_ci", False),  # 183
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    ("utf8", "utf8_unicode_ci", False),  # 192
    ("utf8", "utf8_icelandic_ci", False),  # 193
    ("utf8", "utf8_latvian_ci", False),  # 194
    ("utf8", "utf8_romanian_ci", False),  # 195
    ("utf8", "utf8_slovenian_ci", False),  # 196
    ("utf8", "utf8_polish_ci", False),  # 197
    ("utf8", "utf8_estonian_ci", False),  # 198
    ("utf8", "utf8_spanish_ci", False),  # 199
    ("utf8", "utf8_swedish_ci", False),  # 200
    ("utf8", "utf8_turkish_ci", False),  # 201
    ("utf8", "utf8_czech_ci", False),  # 202
    ("utf8", "utf8_danish_ci", False),  # 203
    ("utf8", "utf8_lithuanian_ci", False),  # 204
    ("utf8", "utf8_slovak_ci", False),  # 205
    ("utf8", "utf8_spanish2_ci", False),  # 206
    ("utf8", "utf8_roman_ci", False),  # 207
    ("utf8", "utf8_persian_ci", False),  # 208
    ("utf8", "utf8_esperanto_ci", False),  # 209
    ("utf8", "utf8_hungarian_ci", False),  # 210
    ("utf8", "utf8_sinhala_ci", False),  # 211
    ("utf8", "utf8_german2_ci", False),  # 212
    ("utf8", "utf8_croatian_ci", False),  # 213
    ("utf8", "utf8_unicode_520_ci", False),  # 214
    ("utf8", "utf8_vietnamese_ci", False),  # 215
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    ("utf8", "utf8_general_mysql500_ci", False),  # 223
    ("utf8mb4", "utf8mb4_unicode_ci", False),  # 224
    ("utf8mb4", "utf8mb4_icelandic_ci", False),  # 225
    ("utf8mb4", "utf8mb4_latvian_ci", False),  # 226
    ("utf8mb4", "utf8mb4_romanian_ci", False),  # 227
    ("utf8mb4", "utf8mb4_slovenian_ci", False),  # 228
    ("utf8mb4", "utf8mb4_polish_ci", False),  # 229
    ("utf8mb4", "utf8mb4_estonian_ci", False),  # 230
    ("utf8mb4", "utf8mb4_spanish_ci", False),  # 231
    ("utf8mb4", "utf8mb4_swedish_ci", False),  # 232
    ("utf8mb4", "utf8mb4_turkish_ci", False),  # 233
    ("utf8mb4", "utf8mb4_czech_ci", False),  # 234
    ("utf8mb4", "utf8mb4_danish_ci", False),  # 235
    ("utf8mb4", "utf8mb4_lithuanian_ci", False),  # 236
    ("utf8mb4", "utf8mb4_slovak_ci", False),  # 237
    ("utf8mb4", "utf8mb4_spanish2_ci", False),  # 238
    ("utf8mb4", "utf8mb4_roman_ci", False),  # 239
    ("utf8mb4", "utf8mb4_persian_ci", False),  # 240
    ("utf8mb4", "utf8mb4_esperanto_ci", False),  # 241
    ("utf8mb4", "utf8mb4_hungarian_ci", False),  # 242
    ("utf8mb4", "utf8mb4_sinhala_ci", False),  # 243
    ("utf8mb4", "utf8mb4_german2_ci", False),  # 244
    ("utf8mb4", "utf8mb4_croatian_ci", False),  # 245
    ("utf8mb4", "utf8mb4_unicode_520_ci", False),  # 246
    ("utf8mb4", "utf8mb4_vietnamese_ci", False),  # 247
    ("gb18030", "gb18030_chinese_ci", True),  # 248
    ("gb18030", "gb18030_bin", False),  # 249
    ("gb18030", "gb18030_unicode_520_ci", False),  # 250
]

