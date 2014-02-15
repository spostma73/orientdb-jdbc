package com.orientechnologies.orient.jdbc;


import org.junit.Test;

import java.sql.SQLException;
import java.util.Collection;
import java.util.regex.Pattern;

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
public class H2SQLTranslationTest
{
    @Test
    public void testCreateStatements() throws SQLException
    {
        String stmt = "create table table1 (string1 varchar(10), string2 varchar(20) not null, string3 varchar(20) null, number1 int, number2 decimal(15, 2), date1 timestamp)";
        Collection<String> queries = H2.translate(stmt);
        assert queries != null;
        for(String q : queries)
        {
            System.out.print(q);
            System.out.println(';');
        }
    }
}
