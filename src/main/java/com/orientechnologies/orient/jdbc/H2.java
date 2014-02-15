package com.orientechnologies.orient.jdbc;

import org.apache.commons.lang.StringUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/*
 * Copyright 2014 Sander Postma  Magsoft BV
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class H2
{
    private static final Set<String> supportedWords = new HashSet<String>();
    private static final Set<String> stripWords = new HashSet<String>();
    private static final Map<String, String> typeMap = new ConcurrentHashMap<String, String>();
    private static final String SPACE = " ";
    private static final char SPACE_CHAR = ' ';
    private static final String QUOTE = "'";
    private static final String QUOTE_ALT = "`";
    private static final String BRACE_OPEN = "(";
    private static final String BRACE_CLOSED = ")";
    private static final String COMMA = ",";
    private static final String SEMICOLON = ";";
    private static final String TABLE = "TABLE";
    private static final String CLASS = "CLASS";
    private static final String NOT = "NOT";
    private static final String NOTNULL = "NOTNULL";
    private static final String NULL = "NULL";
    private static final String[] SUPPORTED_WORDS = new String[]{"SELECT", "INSERT", "UPDATE", "DELETE", "FROM", "WHERE", "AS",
            "INTO", "VALUES", "SET", "REMOVE", "AND", "OR", "NULL", "ORDER", "BY", "LIMIT", "NOT", "SUM", "SKIP", "ASC", "DESC",
            "CREATE", "ALTER", "DROP", "INDEX"};
    private static final String[] STRIP_WORDS = new String[]{"  ", "\r\n", "\r", "\n", "\t"};
    private static final String[] TYPE_MAPPINGS = new String[]{
            "VARCHAR", "STRING",
            "INT", "INTEGER",
            "BOOLEAN", "BOOLEAN",
            "TINYINT", "BYTE",
            "SMALLINT", "SHORT",
            "BIGINT", "LONG",
            "IDENTITY", "",
            "DECIMAL", "FLOAT",
            "DOUBLE", "DOUBLE",
            "REAL", "FLOAT",
            "TIME", "DATE",
            "DATE", "DATE",
            "TIMESTAMP", "DATE",
            "BINARY", "BINARY",
            "OTHER", "BINARY",
            "VARCHAR", "STRING",
            "VARCHAR_IGNORECASE", "",
            "CHAR", "STRING",
            "BLOB", "BINARY",
            "CLOB", "STRING",
            "UUID", "STRING",
            "ARRAY", "EMBEDDEDLIST",
            "GEOMETRY", "STRING"};

/*
| <RANGE: ("r" | "R") ("a" | "A") ("n" | "N") ("g" | "G") ("e" | "E")>
 */

    static
    {
        supportedWords.addAll(Arrays.asList(SUPPORTED_WORDS));
        stripWords.addAll(Arrays.asList(STRIP_WORDS));
        for (int i = 0; i < TYPE_MAPPINGS.length; i += 2)
            typeMap.put(TYPE_MAPPINGS[i], TYPE_MAPPINGS[i + 1]);
    }

    public static Collection<String> translate(String sql) throws SQLException
    {
        List<String> sections = new ArrayList<String>();
        if (StringUtils.isNotEmpty(sql))
        {
            boolean quoted = false;
            int bracesOpen = 0;
            boolean create = false;
            boolean type = false;
            boolean typeLen = false;
            boolean not = false;
            int max = 0;
            String lastWord = null;
            String className = null;
            String fieldName = null;
            boolean classNameAppended = false;
            StringBuffer section = new StringBuffer();

            for (final StringTokenizer splitter = new StringTokenizer(sql, " ();,'`\r\n\t", true); splitter.hasMoreTokens(); )
            {
                String w = splitter.nextToken();
                if (w.length() == 0)
                    continue;

                if (!quoted)
                {
                    w = w.toUpperCase();
                    if (stripWords.contains(w) || (SPACE.equals(w)))
                        section.append(SPACE_CHAR);
                    else if (QUOTE.equals(w) || QUOTE_ALT.equals(w))
                    {
                        section.append(w);
                        if (QUOTE_ALT.equals(w) && !quoted)
                            w = QUOTE;
                        if (QUOTE.equals(w) && !QUOTE.equals(lastWord))
                            quoted = !quoted;
                    } else if (BRACE_OPEN.equals(w))
                    {
                        bracesOpen++;
                        if (create)
                        {
                            trim(section);
                            if (!type)
                            {
                                sections.add(section.toString());
                                section = new StringBuffer("CREATE PROPERTY ");
                                section.append(className).append('.');
                            } else
                            {
                                sections.add(section.toString());
                                section = new StringBuffer("ALTER PROPERTY ");
                                section.append(className).append('.');
                                section.append(fieldName).append(" MAX ");
                                typeLen = true;
                            }
                        } else
                            section.append(w);

                    } else if (BRACE_CLOSED.equals(w))
                    {
                        bracesOpen--;
                        if (create)
                        {
                            if (typeLen)
                            {
                                trim(section);
                                section.append(SPACE_CHAR).append(max);
                                typeLen = false;
                                max=0;
                                section.append(SPACE_CHAR);
                                continue;
                            } else if (type)
                            {
                                type = false;
                                classNameAppended = false;
                            } else
                                create = false;
                        }
                        section.append(w);
                    } else if (COMMA.equals(w))
                    {
                        if (create)
                        {
                            if (type)
                            {
                                if (!typeLen)
                                {
                                    trim(section);
                                    sections.add(section.toString());
                                    section = new StringBuffer("CREATE PROPERTY ");
                                    section.append(className).append('.');
                                    type = false;
                                    classNameAppended = true;
                                    fieldName = null;
                                } else
                                    max++;
                            } else
                                section.append(w);
                        }
                    } else if (create)
                    {
                        if (type)
                        {
                            boolean suppw = supportedWords.contains(w);
                            if (typeLen || suppw)
                            {
                                if (suppw)
                                {
                                    if (NOT.equals(w))
                                    {
                                        not = true;
                                        continue;
                                    } else
                                    {
                                        trim(section);
                                        sections.add(section.toString());
                                        section = new StringBuffer("ALTER PROPERTY ");
                                        section.append(className).append('.');
                                        section.append(fieldName);
                                        if (not)
                                        {
                                            section.append(" NOT");
                                            not = false;
                                        }
                                        if(NULL.equals(w))
                                        {
                                            if(section.lastIndexOf(NOT) == section.length()-3)
                                                section.append(w).append("=TRUE");
                                            else
                                                section.append(SPACE_CHAR).append(NOT).append(w).append("=FALSE");
                                        }
                                        else
                                            section.append(w);
                                    }
                                } else
                                    max += Integer.parseInt(w);
                            } else
                            {
                                final String mtype = typeMap.get(w);
                                if (StringUtils.isEmpty(mtype))
                                    throw new SQLException(String.format("Sorry, type %s is not supported by Orient at the moment.", w));
                                section.append(mtype);
                            }

                        } else if (!classNameAppended)
                        {
                            className = w;
                            section.append(className);
                            classNameAppended = true;
                        } else
                        {
                            if (section.charAt(section.length() - 1) == SPACE_CHAR)
                                section.deleteCharAt(section.length() - 1);
                            section.append(w);
                            fieldName = w;
                            type = true;
                        }
                    } else if (supportedWords.contains(w))
                        section.append(w);
                    else if (SEMICOLON.equals(w))
                    {
                        trim(section);
                        sections.add(section.toString());
                        section = new StringBuffer();
                    } else if (TABLE.equals(w))
                    {
                        section.append(CLASS);
                        create = true;
                    } else
                        section.append(w);
                } else
                    section.append(w);
            }
        }
        return sections;
    }

    private static void trim(StringBuffer section)
    {
        while (section.charAt(section.length() - 1) == SPACE_CHAR)
            section.deleteCharAt(section.length() - 1);
    }

}
