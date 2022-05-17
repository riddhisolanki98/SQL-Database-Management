select segment_name,segment_type,sum(bytes/1024/1024) sum_mb
from user_segments
where segment_name in (select table_name from user_tables)
or segment_name in (select index_name from user_indexes)
group by segment_name,segment_type
having sum(bytes/1024/1024)>1
order by sum_mb desc;

select * from project1_book_copies_load;
select * from project1_books_load;
select * from project1_borrowers_load;
select * from project1_library_branch_load;

CREATE TABLE PROJECT1_BOOKS AS (select ISBN10, TITLE 
FROM project1_books_load); 
COMMIT; 
ALTER TABLE PROJECT1_BOOKS ADD PRIMARY KEY (ISBN10); 
COMMIT;


CREATE TABLE PROJECT1_AUTHORS AS
(
select row_number() over (order by NAME) as AUTHOR_ID,C.* from (
select NAME from (
SELECT m.author
,REGEXP_SUBSTR (author, '[^,]+', 1, 1) AS NAME
, REGEXP_SUBSTR (author, '[^,]+', 1, 2) AS part_2
, REGEXP_SUBSTR (author, '[^,]+', 1, 3) AS part_3
, REGEXP_SUBSTR (author, '[^,]+', 1, 4) AS part_4
, REGEXP_SUBSTR (author, '[^,]+', 1, 5) AS part_5
FROM project1_books_load m where REGEXP_SUBSTR (author, '[^,]+', 1, 1) is not null)
union
select part_2 from (
SELECT m.author
,REGEXP_SUBSTR (author, '[^,]+', 1, 1) AS NAME
, REGEXP_SUBSTR (author, '[^,]+', 1, 2) AS part_2
, REGEXP_SUBSTR (author, '[^,]+', 1, 3) AS part_3
, REGEXP_SUBSTR (author, '[^,]+', 1, 4) AS part_4
, REGEXP_SUBSTR (author, '[^,]+', 1, 5) AS part_5
FROM project1_books_load m where REGEXP_SUBSTR (author, '[^,]+', 1, 2) is not null)
union
select part_3 from (
SELECT m.author
,REGEXP_SUBSTR (author, '[^,]+', 1, 1) AS NAME
, REGEXP_SUBSTR (author, '[^,]+', 1, 2) AS part_2
, REGEXP_SUBSTR (author, '[^,]+', 1, 3) AS part_3
, REGEXP_SUBSTR (author, '[^,]+', 1, 4) AS part_4
, REGEXP_SUBSTR (author, '[^,]+', 1, 5) AS part_5
FROM project1_books_load m where REGEXP_SUBSTR (author, '[^,]+', 1, 3) is not null)
UNION
select part_4 from (
SELECT m.author
,REGEXP_SUBSTR (author, '[^,]+', 1, 1) AS NAME
, REGEXP_SUBSTR (author, '[^,]+', 1, 2) AS part_2
, REGEXP_SUBSTR (author, '[^,]+', 1, 3) AS part_3
, REGEXP_SUBSTR (author, '[^,]+', 1, 4) AS part_4
, REGEXP_SUBSTR (author, '[^,]+', 1, 5) AS part_5
FROM project1_books_load m where REGEXP_SUBSTR (author, '[^,]+', 1, 4) is not null)
union
select part_5 from (
SELECT m.author
,REGEXP_SUBSTR (author, '[^,]+', 1, 1) AS NAME
, REGEXP_SUBSTR (author, '[^,]+', 1, 2) AS part_2
, REGEXP_SUBSTR (author, '[^,]+', 1, 3) AS part_3
, REGEXP_SUBSTR (author, '[^,]+', 1, 4) AS part_4
, REGEXP_SUBSTR (author, '[^,]+', 1, 5) AS part_5
FROM project1_books_load m where REGEXP_SUBSTR (author, '[^,]+', 1, 5) is not null
))C);

COMMIT;


ALTER TABLE PROJECT1_AUTHORS ADD PRIMARY KEY (AUTHOR_ID); 
COMMIT;

CREATE TABLE PROJECT1_BOOK_AUTHORS AS (
SELECT DENSE_RANK() over (order by NAME) AS author_id, C.isbn10 FROM (
select isbn10, NAME from (
SELECT m.isbn10, m.author
,REGEXP_SUBSTR (author, '[^,]+', 1, 1) AS NAME
, REGEXP_SUBSTR (author, '[^,]+', 1, 2) AS part_2
, REGEXP_SUBSTR (author, '[^,]+', 1, 3) AS part_3
, REGEXP_SUBSTR (author, '[^,]+', 1, 4) AS part_4
, REGEXP_SUBSTR (AUTHOR, '[^,]+', 1, 5) AS part_5

FROM PROJECT1_BOOKS_LOAD m where REGEXP_SUBSTR (author, '[^,]+', 1, 1) is not null)
union
select isbn10, part_2 from (
SELECT m.isbn10, m.author
,REGEXP_SUBSTR (author, '[^,]+', 1, 1) AS NAME
, REGEXP_SUBSTR (author, '[^,]+', 1, 2) AS part_2
, REGEXP_SUBSTR (author, '[^,]+', 1, 3) AS part_3
, REGEXP_SUBSTR (author, '[^,]+', 1, 4) AS part_4
, REGEXP_SUBSTR (AUTHOR, '[^,]+', 1, 5) AS part_5

FROM PROJECT1_BOOKS_LOAD m where REGEXP_SUBSTR (author, '[^,]+', 1, 2) is not null)
union
select isbn10, part_3 from (
SELECT m.isbn10, m.author
,REGEXP_SUBSTR (author, '[^,]+', 1, 1) AS NAME
, REGEXP_SUBSTR (author, '[^,]+', 1, 2) AS part_2
, REGEXP_SUBSTR (author, '[^,]+', 1, 3) AS part_3
, REGEXP_SUBSTR (author, '[^,]+', 1, 4) AS part_4
, REGEXP_SUBSTR (AUTHOR, '[^,]+', 1, 5) AS part_5

FROM PROJECT1_BOOKS_LOAD m where REGEXP_SUBSTR (author, '[^,]+', 1, 3) is not null)
union
select isbn10, part_4 from (
SELECT m.isbn10, m.author
,REGEXP_SUBSTR (author, '[^,]+', 1, 1) AS NAME
, REGEXP_SUBSTR (author, '[^,]+', 1, 2) AS part_2
, REGEXP_SUBSTR (author, '[^,]+', 1, 3) AS part_3
, REGEXP_SUBSTR (author, '[^,]+', 1, 4) AS part_4
, REGEXP_SUBSTR (AUTHOR, '[^,]+', 1, 5) AS part_5

FROM PROJECT1_BOOKS_LOAD m where REGEXP_SUBSTR (author, '[^,]+', 1, 4) is not null)
union
select isbn10, part_5 from (
SELECT m.isbn10, m.AUTHOR
,REGEXP_SUBSTR (AUTHOR, '[^,]+', 1, 1) AS NAME , 
REGEXP_SUBSTR (AUTHOR, '[^,]+', 1, 2) AS part_2 , 
REGEXP_SUBSTR (AUTHOR, '[^,]+', 1, 3) AS part_3 , 
REGEXP_SUBSTR (AUTHOR, '[^,]+', 1, 4) AS part_4 , 
REGEXP_SUBSTR (AUTHOR, '[^,]+', 1, 5) AS part_5
FROM PROJECT1_BOOKS_LOAD m where REGEXP_SUBSTR (AUTHOR, '[^,]+', 1, 5) is not null) )C); 
COMMIT;


ALTER TABLE PROJECT1_BOOK_AUTHORS 
ADD CONSTRAINT AUTHOR_ID_FK FOREIGN KEY (AUTHOR_ID) 
REFERENCES PROJECT1_AUTHORS(AUTHOR_ID); 
COMMIT; 

ALTER TABLE PROJECT1_BOOK_AUTHORS 
ADD CONSTRAINT isbn10_FK FOREIGN KEY (ISBN10) 
REFERENCES PROJECT1_BOOKS(ISBN10); 
COMMIT;

CREATE TABLE PROJECT1_LIBRARY_BRANCH as 
(select * from project1_library_branch_load);
COMMIT ;

alter table PROJECT1_LIBRARY_BRANCH 
add constraint branchid_pk primary key (branch_id);
COMMIT;


create table BOOKLOAD_TMP
(book_id varchar2(10),
branch_id number,
no_of_copies NUMBER);
COMMIT;

DECLARE 
   rep_cnt NUMBER;
   CURSOR c_trx
   IS
    SELECT 
        book_id,branch_id, no_of_copies  
        FROM project1_book_copies_load;
BEGIN 
   FOR r_trx IN c_trx
   LOOP

    rep_cnt := r_trx.no_of_copies;
    LOOP
        IF  rep_cnt <= 0
        THEN
            EXIT;
        END IF;
		INSERT INTO BOOKLOAD_TMP VALUES (r_trx.book_ID,r_trx.branch_id,r_trx.no_of_copies);
        rep_cnt := rep_cnt-1;
    END LOOP;
  END LOOP;
COMMIT;
END;
/


CREATE TABLE PROJECT1_BOOK_COPIES 
AS (SELECT row_number() over (order by book_id, branch_id) as book_id,
book_id as isbn10, branch_id, no_of_copies 
FROM BOOKLOAD_TMP);
COMMIT;


alter table PROJECT1_BOOK_COPIES add constraint bookid_pk primary key (BOOK_ID);
COMMIT;
ALTER TABLE PROJECT1_BOOK_COPIES
ADD CONSTRAINT isbn10bc_FK FOREIGN KEY (ISBN10) REFERENCES
PROJECT1_BOOKS(ISBN10);
COMMIT;

ALTER TABLE PROJECT1_BOOK_COPIES
ADD CONSTRAINT branch_FK FOREIGN KEY (BRANCH_ID) REFERENCES
project1_library_branch(branch_id);
COMMIT;

 
CREATE TABLE PROJECT1_BORROWER 
as (select ID0000ID AS CARD_NO, SSN, FIRST_NAME AS FNAME, LAST_NAME AS LNAME, 
ADDRESS, PHONE from project1_borrowers_load); 
COMMIT; 

alter table PROJECT1_BORROWER 
add constraint cardno_pk primary key (CARD_NO); 
COMMIT;    

create table PROJECT1_book_loans as 
( (select c.* from ( Select ROW_NUMBER() OVER(order by bl.card_no) as loan_id, 
bl.card_no,bc.BOOK_ID, SYSDATE - ROUND(DBMS_RANDOM.value(1,60),0) as DATE_OUT, 
(SYSDATE - ROUND(DBMS_RANDOM.value(1,60),0)) + ROUND(DBMS_RANDOM.value(1,90),0) 
AS DUE_DATE, 
((SYSDATE - ROUND(DBMS_RANDOM.value(1,60),0)) + ROUND(DBMS_RANDOM.value(1,90),0)) + ROUND(DBMS_RANDOM.value(1,120),0) 
AS DATE_IN 
from (SELECT * FROM 
( select * from PROJECT1_BORROWER ORDER BY DBMS_RANDOM.RANDOM ) 
where rownum<200) bl, 
(SELECT * FROM 
( select * from PROJECT1_BOOK_COPIES ORDER BY DBMS_RANDOM.RANDOM ) 
where rownum < 100) bc 
ORDER BY DBMS_RANDOM.RANDOM DESC 
FETCH FIRST 400 ROWS ONLY) c)); 
COMMIT;


create sequence loan_id increment by 1; 
COMMIT; 

UPDATE PROJECT1_book_loans SET 
loan_id = loan_id.nextval; 
COMMIT; 

ALTER table PROJECT1_book_loans 
Add Constraint loanid_pk primary key (loan_id); 
COMMIT; 

ALTER TABLE PROJECT1_book_loans 
ADD CONSTRAINT cardno_FK FOREIGN KEY (card_no) 
REFERENCES PROJECT1_BORROWER(CARD_NO); 
COMMIT; 

ALTER TABLE PROJECT1_book_loans 
ADD CONSTRAINT BOOKIDLOAN_FK FOREIGN KEY (BOOK_ID) 
REFERENCES PROJECT1_BOOK_COPIES(BOOK_ID); 
COMMIT;



create table project1_fines as
 (select c.* from
((SELECT LOAN_ID,ROUND(DATE_IN-DUE_DATE)
AS DELAY_BY,
ROUND(DATE_IN-DUE_DATE)*0.50 
AS FINE, 
(case 
     when DBMS_RANDOM.RANDOM > 0 
       then 'Y' 
     else 
            'N' 
     End) AS PAID 
 
 FROM project1_book_loans 
 where ROUND(DATE_IN-DUE_DATE)*5 > 1 
 GROUP BY LOAN_ID,card_no,ROUND(DATE_IN-DUE_DATE) ,ROUND(DATE_IN-DUE_DATE)*0.50 ) 
 FETCH first 50 rows only) c); 
 COMMIT;


ALTER table PROJECT1_FINES 
Add constraint fINEloanid_pk primary key (loan_id); 
COMMIT; 

ALTER TABLE PROJECT1_FINES 
ADD CONSTRAINT FINESloan_FK FOREIGN KEY (loan_id) 
REFERENCES PROJECT1_book_loans(loan_id); 
COMMIT;



select pb.isbn10, pb.title, pa.name, plb.branch_name 
from project1_books pb 
inner join project1_book_authors ba on pb.isbn10 = ba.isbn10
inner join project1_authors pa on ba.author_id = pa.author_id 
inner join project1_book_copies bc0 on pb.isbn10 = bc0.isbn10 
inner join project1_library_branch plb on plb.branch_id = bc0.branch_id 
where 
(pa.name) like '%'||:search_for_author||'%' 
and pb.isbn10 like '%'||:search_for_isbn||'%' 
and (pb.title) like '%'||:search_for_title||'%';


SELECT * FROM 
(SELECT PF.LOAN_ID,PBL.CARD_NO,bor.FNAME,bor.LNAME,PF.PAID,pf.FINE 
FROM PROJECT1_FINES PF 
INNER JOIN PROJECT1_BOOK_LOANs PBL ON PF.LOAN_ID= PBL.LOAN_ID 
INNER JOIN PROJECT1_BORROWER bor ON PBL.CARD_NO = bor.CARD_NO AND pf.paid IN 'N' 
ORDER BY pf.fine DESC) 
WHERE ROWNUM<= 10;


SELECT * FROM 
(SELECT PF.LOAN_ID,PBL.CARD_NO,bor.FNAME,bor.LNAME,PF.PAID,pf.FINE 
FROM PROJECT1_FINES PF 
INNER JOIN PROJECT1_BOOK_LOANs PBL ON PF.LOAN_ID= PBL.LOAN_ID 
INNER JOIN PROJECT1_BORROWER bor ON PBL.CARD_NO = bor.CARD_NO AND pf.paid IN 'Y' 
ORDER BY pf.fine DESC) 
WHERE ROWNUM<= 10;


SELECT * FROM 
(SELECT PF.DELAY_BY,PF.LOAN_ID,pf.fine,BL.CARD_NO,BC.ISBN10,B.TITLE 
FROM PROJECT1_FINES PF 
JOIN PROJECT1_BOOK_LOANS BL ON PF.LOAN_ID=BL.LOAN_ID 
JOIN PROJECT1_BOOK_COPIES BC ON BL.BOOK_ID = BC.BOOK_ID 
JOIN PROJECT1_BOOKS B ON BC.ISBN10=B.ISBN10 
ORDER BY DELAY_BY ASC) 
WHERE ROWNUM <=10;



















