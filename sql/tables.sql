/**
 * Copyright 2021 Tamado Sitohang <ramot@ramottamado.dev>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

CREATE TABLE transactions
(
    "src_acct" varchar(9),
    "dest_acct" varchar(9),
    "trx_type" varchar(8),
    "amount" double precision,
    "trx_timestamp" timestamp
)
;

INSERT INTO transactions
    ("src_acct", "dest_acct", "trx_type", "amount", "trx_timestamp")
VALUES
    ('067637881', NULL, 'PURCHASE', 120000, '2021-01-09 11:30:34'),
    ('067345125', '099912123', 'TRANSFER', 10000, '2021-02-01 10:11:12'),
    ('087335125', '099912123', 'TRANSFER', 10000, '2021-02-01 11:15:12'),
    ('067637881', NULL, 'PURCHASE', 150000, '2021-02-01 13:30:34'),
    ('034716192', NULL, 'PURCHASE', 150000, '2021-02-01 15:48:24'),
    ('087335125', NULL, 'PURCHASE', 11000, '2021-02-02 14:15:12')
;

CREATE TABLE customers
(
    "cif" varchar(12) primary key unique,
    "acct_number" varchar(9) unique,
    "first_name" varchar(100),
    "last_name" varchar(100),
    "city" varchar(100)
)
;

INSERT INTO customers
    ("cif", "acct_number", "first_name", "last_name", "city")
VALUES
    ('029817127819', '067637881', 'Taufiq', 'Maulana', 'Jakarta'),
    ('018374828921', '067345125', 'Agus', 'Hidayatullah', 'Karawaci'),
    ('018273819192', '099912123', 'Kamal', 'Rasyid', 'Jakarta'),
    ('018374651812', '087335125', 'Muhamad', 'Ikbal', 'Jakarta'),
    ('024819172821', '071819471', 'Yusfi', 'Ikhwan', 'Jakarta')
;
