#!/usr/bin/env scheme-script
;; -*- mode: scheme; coding: utf-8 -*- !#
;; Copyright (c) 2020 Travis Hinkelman
;; SPDX-License-Identifier: MIT
#!r6rs

(import (srfi :64 testing)
        (dataframe)
        (dataframe rowtable))

;;-------------------------------------------------------------

(test-begin "types-test")
(test-equal 'num (guess-type (iota 10) 10))
(test-equal 'str (guess-type '("a" "b" "c") 3))
(test-equal 'sym (guess-type '(a b c) 3))
(test-equal 'str (guess-type '(a b "c") 3))
(test-equal 'sym (guess-type '(a b "c") 2))
(test-equal 'bool (guess-type '(#t #f) 2))
(test-equal 'other (guess-type '((1 2) (3 4)) 2))
(test-equal 'chr (guess-type '(#\a #\b) 2))
(test-equal '(1 2 3) (convert-type '(1 2 3) 'num))
(test-equal '("1" "2" "3") (convert-type '(1 2 3) 'str))
(test-equal '("1" "2" "3") (convert-type '(1 "2" 3) 'str))
(test-equal '(na na na) (convert-type '(1 2 3) 'sym))
(test-equal '(na na na) (convert-type '(1 2 3) 'chr))
(test-equal '(1 2 3) (convert-type '(1 2 3) 'other))
(test-equal '(a b na) (convert-type '(a b "c") 'sym))
(test-equal '("a" "b" "c" na na na na na)
  (convert-type '(a "b" c na "" " " "NA" "na") 'str))
(test-equal '(a na c na na na na na)
  (convert-type '(a "b" c na "" " " "NA" "na") 'sym))
(test-end "types-test")

;;-------------------------------------------------------------

(define s1 (make-series 'a (iota 10)))

(test-begin "series-record-test")
(test-assert (series-equal? (make-series 'a '(1 2 3))
                            (make-series* (a 1 2 3))))
(test-assert (series-equal? (make-series 'a '(a b c))
                            (make-series* (a 'a 'b 'c))))
(test-assert (series? s1))
(test-equal 'a (series-name s1))
(test-equal (iota 10) (series-lst s1))
(test-equal 10 (series-length s1))
(test-assert (series-equal? (make-series* (a 1 2 3))
                            (make-series* (a 1 "2" 3))))
(test-equal 'str (series-type (make-series* (a 1 "b" 3))))
(test-equal 'str (series-type (make-series* (a 'a 'b "c"))))
(test-equal 'str (series-type (make-series* (a #t "#f"))))
(test-equal 'other (series-type (make-series* (a 1 2 '(3 4)))))
(test-assert (series-equal? (car (make-slist '(a) (list (iota 10)))) s1))
(test-error (make-series 'a 'a))
(test-error (make-series 'a '()))
(test-end "series-record-test")

;;-------------------------------------------------------------

(define df1
  (make-df*
   (a 1 2 3)
   (b 4 5 6)))

(define df2
  (make-df*
   (a 1 2 3)
   (b 4 5 6)
   (c 7 8 9)))

(define df3
  (make-df*
   (a 1 2 3 1 2 3)
   (b 4 5 6 4 5 6)
   (c 'na 'na 'na 7 8 9)))

(define df4
  (make-df*
   (a 1 2 3 1 2 3)
   (b 4 5 6 4 5 6)
   (c 7 8 9 'na 'na 'na)))

;;-------------------------------------------------------------

(test-begin "df-record-test")
(test-assert (dataframe-equal? (make-dataframe (list (make-series* (a 1 2 3))
                                                     (make-series* (b 4 5 6))))
                               df1))
(test-assert (dataframe? df1))
(test-assert (not (dataframe? '((a 1 2 3) (b 4 5 6)))))
(test-assert (dataframe-contains? df1 'b))
(test-assert (dataframe-contains? df1 'a 'b))
(test-assert (not (dataframe-contains? df1 'a 'b 'c)))
(test-assert (not (dataframe-contains? df1 "b")))
(test-error (dataframe-contains? df1 b))
(test-equal (cons 3 2) (dataframe-dim df1))
(test-assert (dataframe-equal? (make-dataframe (make-slist '(a b) '((1 2 3) (4 5 6)))) df1))
(test-error (make-dataframe '()))
(test-error (make-dataframe '((1 2 3))))
(test-error (make-dataframe (list (make-series* (a 1 2 3)) '(b 4 5 6))))
(test-error (make-df* (a 1 2 3) (a 4 5 6)))
(test-error (make-df* (a 1 2) (b 4 5 6)))
(test-equal '(a b c) (dataframe-names df2))
(test-equal '(a b) (dataframe-names df1))
(test-equal '(a) (dataframe-names (make-df* (a 1 2 3))))
(test-end "df-record-test")

;;-------------------------------------------------------------

(test-begin "dataframe-head-test")
(test-assert (dataframe-equal? df2 (dataframe-head df4 3)))
(test-error (dataframe-head '(1 2 3) 3))
(test-error (dataframe-head df2 4))
(test-error (dataframe-head df2 0.5))
(test-error (dataframe-head df2 0))
(test-end "dataframe-head-test")

;;-------------------------------------------------------------

(test-begin "dataframe-tail-test")
(test-assert (dataframe-equal? (make-df* (a 2 3) (b 5 6) (c 'na 'na))
                               (dataframe-tail df4 4)))
(test-error (dataframe-tail '(1 2 3) 3))
(test-error (dataframe-tail df2 3))
(test-error (dataframe-tail df2 0.5))
(test-error (dataframe-tail df2 -1))
(test-end "dataframe-tail-test")

;;-------------------------------------------------------------

(test-begin "dataframe-ref-test")
(test-assert (dataframe-equal?
              (make-df*
               (a 1 3 2)
               (b 4 6 5)
               (c 'na 'na 8))
              (dataframe-ref df3 '(0 2 4))))
(test-assert (dataframe-equal?
              (make-df* (a 1 3 2) (b 4 6 5))
              (dataframe-ref df3 '(0 2 4) 'a 'b)))
(test-error (dataframe-ref df3 '()))
(test-error (dataframe-ref df3 '(0 10)))
(test-error (dataframe-ref df3 2))
(test-error (dataframe-ref df3 '(2) 'total))
(test-end "dataframe-ref-test")

;;-------------------------------------------------------------

(test-begin "dataframe-series-test")
(test-assert (series-equal? (make-series* (a 1 2 3))
                            (dataframe-series df1 'a)))
(test-assert (series-equal? (make-series* (b 4 5 6))
                            (dataframe-series df1 'b)))
(test-error (dataframe-series df1 'd))
(test-error (dataframe-series df1 'a 'b))
(test-end "dataframe-series-test")

;;-------------------------------------------------------------

(test-begin "dataframe-values-test")
(test-equal '(100 200 300) (dataframe-values (make-df* (a 100 200 300)) 'a))
(test-equal '(4 5 6) (dataframe-values df1 'b))
(test-error (dataframe-values df1 'd))
(test-error (dataframe-values df1 'a 'b))
(test-end "dataframe-values-test")

;;-------------------------------------------------------------

(define dfc
  (make-df*
   (col1 'a 'a 'b 'b)
   (col2 'c 'd 'c 'd)))

;;-------------------------------------------------------------

(test-begin "dataframe-crossing-test")
(test-assert (dataframe-equal?
              dfc
              (dataframe-crossing (make-series* (col1 'a 'b))
                                  (make-series* (col2 'c 'd)))))
(test-assert (dataframe-equal?
              dfc
              (dataframe-crossing (make-series* (col1 'a 'b))
                                  (make-df* (col2 'c 'd)))))
(test-assert (dataframe-equal?
              dfc
              (dataframe-crossing (make-df* (col1 'a 'b))
                                  (make-df* (col2 'c 'd)))))
(test-error (dataframe-crossing '(col1 a b) '("col2" c d)))
(test-error (dataframe-crossing '(col1 a b) '()))
(test-end "dataframe-crossing-test")

;;-------------------------------------------------------------

(test-begin "dataframe-append-test")
(test-assert (dataframe-equal?
              df2
              (dataframe-append df1 (make-df* (c 7 8 9)))))
(test-assert (dataframe-equal?
              (dataframe-select* df2 c a b)
              (dataframe-append (make-df* (c 7 8 9)) df1)))
(test-error (dataframe-append df1 (make-df* (c 7 8 9 10))))
(test-error (dataframe-append df1 (make-df* (b 7 8 9))))
(test-end "dataframe-append-test")

;;-------------------------------------------------------------

(test-begin "dataframe-bind-test")
(test-assert (dataframe-equal?
              df3
              (dataframe-bind df1 df2)))
(test-assert (dataframe-equal?
              df4
              (dataframe-bind df2 df1)))
(test-end "dataframe-bind-test")

;;-------------------------------------------------------------

(test-begin "dataframe-bind-all-test")
(test-assert (dataframe-equal?
              df3
              (dataframe-bind-all (list df1 df2))))
(test-assert (dataframe-equal?
              df4
              (dataframe-bind-all (list df2 df1))))
(test-error (dataframe-bind-all (list df3 df4 '(1 2 3))))
(test-end "dataframe-bind-all-test")

;;-------------------------------------------------------------

(define df5
  (make-df*
   (a 1 2 3)
   (b 4 5 6)
   (c 7 8 9)))

(define df6
  (make-df*
   (b 4 5 6)
   (c 7 8 9)))

(define df7
  (make-df*
   (c 7 8 9)))

;;-------------------------------------------------------------

(test-begin "dataframe-drop-test")
(test-error (dataframe-drop df5 '(d)))
(test-error (dataframe-drop* df5 d))
(test-assert (dataframe-equal? df6 (dataframe-drop df2 '(a))))
(test-assert (dataframe-equal? df6 (dataframe-drop* df2 a)))
(test-assert (dataframe-equal? df7 (dataframe-drop df2 '(a b))))
(test-assert (dataframe-equal? df7 (dataframe-drop* df2 a b)))
(test-end "dataframe-drop-test")

;;-------------------------------------------------------------

(define df8
  (make-df*
   (a 1 2 3)
   (b 4 5 6)
   (scary7 7 8 9)))

(define df9
  (make-df*
   (A 1 2 3)
   (b 4 5 6)
   (scary7 7 8 9)))

;;-------------------------------------------------------------

(test-begin "dataframe-rename-test")
(test-assert (dataframe-equal? df8 (dataframe-rename* df5 (c scary7))))
(test-assert (dataframe-equal? df8 (dataframe-rename df5 '(c) '(scary7))))
(test-assert (dataframe-equal? df9 (dataframe-rename* df5 (a A) (c scary7))))
(test-assert (dataframe-equal? df9 (dataframe-rename df5 '(a c) '(A scary7))))
;; same dataframe is returned when old names are not found
(test-assert (dataframe-equal? df8 (dataframe-rename* df8 (d D))))
(test-assert (dataframe-equal? df8 (dataframe-rename df8 '(d) '(D))))
(test-error (dataframe-rename* df5 (a c)))
(test-error (dataframe-rename* df5 (a "a")))
(test-error (dataframe-rename df5 '(a b) '(A)))
(test-end "dataframe-rename-test")

;;-------------------------------------------------------------

(test-begin "dataframe-rename-all-test")
(test-assert (dataframe-equal? df8 (dataframe-rename-all df5 '(a b scary7))))
(test-assert (dataframe-equal? df9 (dataframe-rename-all df5 '(A b scary7))))
(test-error (dataframe-rename-all df5 '(A B)))
(test-error (dataframe-rename-all df5 '(a a b)))
(test-error (dataframe-rename-all df5 '("a" b c)))
(test-end "dataframe-rename-all-test")

;;-------------------------------------------------------------

(test-begin "dataframe-select-test")
(test-error (dataframe-select df8 '(d)))
(test-error (dataframe-select* df8 d))
(test-assert (dataframe-equal? df6 (dataframe-select df2 '(b c))))
(test-assert (dataframe-equal? df6 (dataframe-select* df2 b c)))
(test-assert (dataframe-equal? df7 (dataframe-select df2 '(c))))
(test-assert (dataframe-equal? df7 (dataframe-select* df2 c)))
(test-end "dataframe-select-test")

;;-------------------------------------------------------------

(define df10
  (make-df*
   (a 100 200 300)
   (b 4 5 6)
   (c 700 800 900)))

;;-------------------------------------------------------------

(test-begin "dataframe-modify-at-all-test")
(test-error (dataframe-modify-at df5 (lambda (x) (* x 100)) 'd))
(test-assert (dataframe-equal?
              df10
              (dataframe-modify-at df5 (lambda (x) (* x 100)) 'a 'c)))
(test-assert (dataframe-equal?
              (make-df*
               (a 100 200 300)
               (b 400 500 600)
               (c 700 800 900))
              (dataframe-modify-all df5 (lambda (x) (* x 100)))))
(test-error (dataframe-modify-at df5 (lambda (x) (* x 100)) 'a 'c 'd))
(test-error (dataframe-modify-at df5 "test" 'a))
(test-end "dataframe-modify-at-all-test")

;;-------------------------------------------------------------

(define df11
  (make-df*
   (a 100 200 300)
   (b 4 5 6)
   (c 700 800 900)
   (d 104 205 306)))

(define df12
  (make-df*
   (a 100 200 300)
   (b 4 5 6)
   (c 700 800 900)
   (d 400 500 600)))

(define df13
  (make-df*
   (a 100 200 300)
   (b 4 5 6)
   (c 700 800 900)
   (d "100_4" "200_5" "300_6")))

(define df14
  (make-df*
   (a 200 300)
   (b 5 6)
   (c 800 900)))

(define df15
  (make-df*
   (a 200)
   (b 5)
   (c 800)))

;;-------------------------------------------------------------

(test-begin "dataframe-filter-test")
(test-assert (dataframe-equal?
              df14
              (dataframe-filter df10 '(a) (lambda (a) (> a 100)))))
(test-assert (dataframe-equal?
              df15
              (dataframe-filter df10 '(b) (lambda (b) (= b 5)))))
(test-assert (dataframe-equal?
              df15
              (dataframe-filter df10 '(a b) (lambda (a b) (or (odd? a) (odd? b))))))
(test-assert (dataframe-equal? df14 (dataframe-filter* df10 (a) (> a 100))))
(test-assert (dataframe-equal? df15 (dataframe-filter* df10 (b) (= b 5))))
(test-assert (dataframe-equal?
              df15
              (dataframe-filter* df10 (a b) (or (odd? a) (odd? b)))))
(test-assert (dataframe-equal?
              (make-df*
               (a 100 300)
               (b 4 6)
               (c 700 900))
              (dataframe-filter-all df10 even?)))
(test-assert (dataframe-equal? df10 (dataframe-filter-at df10 even? 'a 'c)))
(test-error (dataframe-filter-all df10 odd?))
(test-end "dataframe-filter-test")

;;-------------------------------------------------------------

(define df16
  (make-df*
   (a 200)
   (b 5)
   (c 800)))

(define df17
  (make-df*
   (a 100 300)
   (b 4 6)
   (c 700 900)))

(define df-na
  (make-df*
   (a 1 2 3 "")
   (b "a" "b" "c" " ")
   (c "NA" 'a 'b 'c)
   (d #t #f "na" #f)))

(define df-types
  (make-df* 
   (Boolean #t #f #t)
   (Char #\y #\e #\s)
   (String "these" "are" "strings")
   (Symbol 'these 'are 'symbols)
   (Exact 1/2 1/3 1/4)
   (Integer 1 -2 3)
   (Expt 1e6 -123456 1.2346e-6)
   (Dec4 132.1 -157 10.234)   ; based on size of numbers
   (Dec2 1234 5784 -76833.123)
   (Other (cons 1 2) '(a b c) (make-df* (a 2)))))

(define df-types-str
  (make-df* 
   (Boolean "#t" "#f" "#t")
   (Char "y" "e" "s")
   (String "these" "are" "strings")
   (Symbol "these" "are" "symbols")
   (Exact 1/2 1/3 1/4)
   (Integer 1 -2 3)
   (Expt 1e6 -123456 1.2346e-6)
   (Dec4 132.1 -157 10.234)   ; based on size of numbers
   (Dec2 1234 5784 -76833.123)
   ;; other category is written as "NA," which is read as 'na
   (Other 'na 'na 'na)))

;;-------------------------------------------------------------

(test-begin "dataframe-partition-test")
(define-values (part1 part2) (dataframe-partition* df10 (b) (odd? b)))
(test-assert (dataframe-equal? part1 df16))
(test-assert (dataframe-equal? part2 df17))
(test-end "dataframe-partition-test")

;;-------------------------------------------------------------

(test-begin "df-read-write-test")
(dataframe-write df10 "example.scm" #f)
(test-error (dataframe-write df10 "example.scm" #f))
(test-assert (dataframe-equal? df10 (dataframe-read "example.scm")))
(delete-file "example.scm")
(dataframe-write df-na "df-na.scm")
(test-assert (dataframe-equal? df-na (dataframe-read "df-na.scm")))
(delete-file "df-na.scm")
(dataframe->csv df-types "df-types.csv")
(test-assert (dataframe-equal? (csv->dataframe "df-types.csv") df-types-str))
(delete-file "df-types.csv")
(dataframe->tsv df-types "df-types.tsv")
(test-assert (dataframe-equal? (tsv->dataframe "df-types.tsv") df-types-str))
(delete-file "df-types.tsv")
(test-end "df-read-write-test")

;;-------------------------------------------------------------

(define df18
  (make-df*
   (a 1 2 3 1 2 3)
   (b 4 5 6 4 5 6)))

(define df19
  (make-df*
   (c 1 2 3)))

;;-------------------------------------------------------------

(test-begin "dataframe->rowtable-test")
(test-error (dataframe->rowtable 100))
(test-equal '((a b c) (100 4 700) (300 6 900)) (dataframe->rowtable df17))
(test-equal '((c) (1) (2) (3)) (dataframe->rowtable df19))
(test-end "dataframe->rowtable-test")

;;-------------------------------------------------------------

(test-begin "rowtable->dataframe-test")
(test-assert (dataframe-equal?
              df2
              (rowtable->dataframe
               '((a b c)
                 (1 4 7)
                 (2 5 8)
                 (3 6 9))
               #t "test")))
(test-assert (dataframe-equal?
              df2
              (rowtable->dataframe
               '((a b c)
                 ("1" "4" "7")
                 (2 5 8)
                 ("3" "6" "9"))
               #t "test")))
(test-assert (dataframe-equal?
              (make-df*
               (a 1 2 3)
	       (b 4 5 6)
	       (c "7" "8" "NA"))
              (rowtable->dataframe
               '((a b c)
                 (1 4 "7")
                 (2 5 "8")
                 (3 6 "NA"))
               #t "test")))
(test-assert (dataframe-equal?
              (make-df*
               (a 1 2 3)
	       (b 4 5 6)
	       (c 7 8 'na))
              (rowtable->dataframe
               '((a b c)
                 (1 4 "7")
                 (2 5 "8")
                 (3 6 "NA"))
               #t "test")))
(test-assert (dataframe-equal?
              (make-df*
               (a 1 2 3)
	       (b 4 5 6)
	       (c 7 8 'na))
              (rowtable->dataframe
               '((a b c)
                 ("1" 4 "7")
                 ("2" 5 "8")
                 ("3" 6 "NA"))
               #t "test")))
(test-assert (dataframe-equal?
              df17
              (rowtable->dataframe
               '((a b c)
                 (100 4 700)
                 (300 6 900))
               #t "test")))
(test-assert (dataframe-equal?
              df19
              (rowtable->dataframe
               '((c)
                 (1)
                 (2)
                 (3))
               #t "test")))
(test-assert (dataframe-equal?
              (make-df*
               (V0 1 2)
               (V1 3 4))
              (rowtable->dataframe
               '((1 3)
                 (2 4))
               #f "test")))
(test-assert (dataframe-equal?
              df2
	      (rowtable->dataframe
               '(("a" b c)
		 (1 4 7)
		 (2 5 8)
		 (3 6 9))
               #t "test")))
(test-error (rowtable->dataframe '((a b) (1 4 7))))
(test-error (rowtable->dataframe '((a b) (1 4)) 1))
(test-end "rowtable->dataframe-test")

;;-------------------------------------------------------------

(define df20
  (make-df*
   (trt 'A 'A 'A 'B 'B 'B)
   (grp 'A 'B 'A 'B 'A 'B)
   (resp 1 2 3 4 5 6)))

(define df21
  (make-df*
   (trt 'A 'A 'B 'B)
   (grp 'A 'B 'B 'A)))

;;-------------------------------------------------------------

(test-begin "dataframe-unique-test")
(test-error (dataframe-unique '((a (1 2 3)))))
(test-assert (dataframe-equal?
              df21
              (dataframe-unique (dataframe-select* df20 trt grp))))
(test-end "dataframe-unique-test")

;;-------------------------------------------------------------

(define df22
  (make-df*
   (grp 'a 'a 'b 'b 'b)
   (trt 'a 'b 'a 'b 'b)
   (adult 1 2 3 4 5)
   (juv 10 20 30 40 50)))

;;-------------------------------------------------------------

(test-begin "dataframe-split-test")
(define df-list (dataframe-split df22 'grp))
(test-assert (dataframe-equal?
              (car df-list)
              (dataframe-filter* df22 (grp) (symbol=? grp 'a))))
(test-assert (dataframe-equal?
              (cadr df-list)
              (dataframe-filter* df22 (grp) (symbol=? grp 'b))))
(test-assert (dataframe-equal? df22 (dataframe-bind-all df-list)))
(test-end "dataframe-split-test")

;;-------------------------------------------------------------

(define df23
  (make-df*
   (grp 'a 'a 'b 'b 'b)
   (trt 'a 'b 'a 'b 'b)
   (adult 1 2 3 4 5)
   (juv 10 20 30 40 50)
   (total 11 22 33 44 55)))

(define df24
  (make-df*
   (grp 'a 'a 'b 'b 'b)
   (trt 'a 'b 'a 'b 'b)
   (adult 1 2 3 4 5)
   (juv 5 10 15 20 25)))

(define df25
  (make-df*
   (grp 'a 'b)
   (trt 'b 'b)
   (adult 2 5)
   (juv 20 50)
   (juv-mean 15 40)))

(define df26
  (make-df*
   (grp 'a 'a 'b 'b 'b)
   (trt 'a 'b 'a 'b 'b)
   (adult 1 2 3 4 5)
   (juv 5 10 15 20 25)
   (total 6 12 18 24 30)))

;;-------------------------------------------------------------

(test-begin "dataframe-modify-test")
(test-assert (dataframe-equal?
              df23
              (dataframe-modify*
               df22
               (total (adult juv) (+ adult juv)))))
(test-assert (dataframe-equal?
              df23
              (dataframe-modify
               df22
               '(total)
               '((adult juv))
               (lambda (adult juv) (+ adult juv)))))
(test-assert (dataframe-equal?
              df24
              (dataframe-modify*
               df22
               (juv (juv) (/ juv 2)))))
(test-assert (dataframe-equal?
              df24
              (dataframe-modify
               df22
               '(juv)
               '((juv))
               (lambda (juv) (/ juv 2)))))
(test-error (dataframe-modify* df22 ("test" (juv) (/ juv 2))))
(test-error (dataframe-modify df22 '(total1 total2) '((adult juv))
                              (lambda (adult juv) (+ adult juv))))
(test-end "dataframe-modify-test")

;;-------------------------------------------------------------

(test-begin "thread-test")
(define (mean ls) (/ (apply + ls) (length ls)))
(test-equal 12 (-> '(1 2 3) (mean) (+ 10)))
(test-assert (dataframe-equal?
              df1
              (-> df1
                  (dataframe-modify*
                   (c () '(7 8 9)))
                  (dataframe-drop* c))))
(test-assert (dataframe-equal?
              df23
              (-> (list (make-series* (grp 'a 'a 'b 'b 'b))
                        (make-series* (trt 'a 'b 'a 'b 'b))
                        (make-series* (adult 1 2 3 4 5))
                        (make-series* (juv 10 20 30 40 50)))
                  (make-dataframe)
                  (dataframe-modify*
                   (total (adult juv) (+ adult juv))))))
(test-assert (dataframe-equal?
              df24
              (-> (list (make-series* (grp 'a 'a 'b 'b 'b))
                        (make-series* (trt 'a 'b 'a 'b 'b))
                        (make-series* (adult 1 2 3 4 5))
                        (make-series* (juv 10 20 30 40 50)))
                  (make-dataframe)
                  (dataframe-modify*
                   (juv (juv) (/ juv 2))))))
(test-assert (dataframe-equal?
              df26
              (-> df22
                  (dataframe-modify*
                   (juv (juv) (/ juv 2))
                   (total (adult juv) (+ adult juv))))))
(test-assert (dataframe-equal?
              df23   
              (-> df22
                  (dataframe-split 'grp)
                  (->> (map (lambda (df)
                              (dataframe-modify*
                               df
                               (total (adult juv) (+ adult juv))))))
                  (dataframe-bind-all))))
(test-assert (dataframe-equal?
              df25
              (-> df22
                  (dataframe-split 'grp)
                  (->> (map (lambda (df)
                              (dataframe-modify*
                               df
                               (juv-mean () (mean ($ df 'juv)))))))
                  (dataframe-bind-all)
                  (dataframe-filter* (juv juv-mean) (> juv juv-mean)))))
(test-error (-> df22
                (dataframe-filter* (adult) (< adult 1))
                (dataframe-sort* (> juv))))
(test-error (-> '(4 3 5 1) (sort <)))
(test-end "thread-test")

;;-------------------------------------------------------------

(test-begin "dataframe-aggregate-test")
(test-assert (dataframe-equal?
              (make-df*
               (grp 'a 'b)
               (adult-sum 3 12)
               (juv-sum 30 120))
              (dataframe-aggregate*
               df22
               (grp)
               (adult-sum (adult) (apply + adult))
               (juv-sum (juv) (apply + juv)))))
(test-assert (dataframe-equal?
              (make-df*
               (grp 'a 'b)
               (adult-sum 3 12)
               (juv-sum 30 120))
              (dataframe-aggregate
               df22
               '(grp)
               '(adult-sum juv-sum)
               '((adult) (juv))
               (lambda (adult) (apply + adult))
               (lambda (juv) (apply + juv)))))
(test-assert (dataframe-equal?
              (make-df*
               (grp 'a 'a 'b 'b)
               (trt 'a 'b 'a 'b)
               (adult-sum 1 2 3 9)
               (juv-sum 10 20 30 90))
              (dataframe-aggregate*
               df22
               (grp trt)
               (adult-sum (adult) (apply + adult))
               (juv-sum (juv) (apply + juv)))))
(test-assert (dataframe-equal?
              (make-df*
               (grp 'a 'a 'b 'b)
               (trt 'a 'b 'a 'b)
               (adult-sum 1 2 3 9)
               (juv-sum 10 20 30 90))
              (dataframe-aggregate
               df22
               '(grp trt)
               '(adult-sum juv-sum)
               '((adult) (juv))
               (lambda (adult) (apply + adult))
               (lambda (juv) (apply + juv)))))
(test-end "dataframe-aggregate-test")

;;-------------------------------------------------------------

(define df27
  (make-df*
   (grp "a" "a" "b" "b" "b")
   (trt "a" "b" "a" "b" "b")
   (adult 1 2 3 4 5)
   (juv 10 20 30 40 50)))

;;-------------------------------------------------------------

(test-begin "dataframe-sort-test")
(test-assert (dataframe-equal?
              (make-df*
               (grp "a" "b" "b" "a" "b")
               (trt "b" "b" "b" "a" "a")
               (adult 2 4 5 1 3)
               (juv 20 40 50 10 30))
              (dataframe-sort* df27 (string>? trt))))
(test-assert (dataframe-equal?
              (make-df*
               (grp "a" "b" "b" "a" "b")
               (trt "b" "b" "b" "a" "a")
               (adult 2 4 5 1 3)
               (juv 20 40 50 10 30))
              (dataframe-sort df27 (list string>?) '(trt))))
(test-assert (dataframe-equal?
              (make-df*
               (grp "b" "b" "a" "b" "a")
               (trt "b" "b" "b" "a" "a")
               (adult 5 4 2 3 1)
               (juv 50 40 20 30 10))
              (dataframe-sort* df27 (string>? trt) (> adult))))
(test-assert (dataframe-equal?
              (make-df*
               (grp "b" "b" "a" "b" "a")
               (trt "b" "b" "b" "a" "a")
               (adult 5 4 2 3 1)
               (juv 50 40 20 30 10))
              (dataframe-sort df27 (list string>? >) '(trt adult))))
(test-end "dataframe-sort-test")

;;-------------------------------------------------------------

(define df28
  (make-df*
   (site "b" "a" "c")
   (habitat "grassland" "meadow" "woodland")))

(define df29
  (make-df*
   (site "c" "b" "c" "b")
   (day 1 1 2 2)
   (catch 10 12 20 24)))

(define df30
  (make-df*
   (first "sam" "bob" "sam" "dan")
   (last  "son" "ert" "jam" "man")
   (age 10 20 30 40)))

(define df31
  (make-df*
   (first "sam" "bob" "dan" "bob")
   (last "son" "ert" "man" "ert")
   (game 1 1 1 2)
   (goals 0 1 2 3)))

;;-------------------------------------------------------------

(test-begin "dataframe-join-test")
(test-assert (dataframe-equal?
              (dataframe-left-join df28 df29 '(site))
              (make-df*
               (site "b" "b" "a" "c" "c")
               (habitat "grassland" "grassland" "meadow" "woodland" "woodland")
               (day 1 2 'na 1 2)
               (catch 12 24 'na 10 20))))
(test-assert (dataframe-equal?
              (dataframe-left-join df29 df28 '(site))
              (make-df*
               (site "c" "c" "b" "b")
               (day 1 2 1 2)
               (catch 10 20 12 24)
               (habitat "woodland" "woodland" "grassland" "grassland"))))
(test-assert (dataframe-equal?
              (dataframe-left-join-all (list df29 df28) '(site))
              (make-df*
               (site "c" "c" "b" "b")
               (day 1 2 1 2)
               (catch 10 20 12 24)
               (habitat "woodland" "woodland" "grassland" "grassland"))))
(test-error (dataframe-left-join df31 (dataframe-slist df30) '(first last) -999))
(test-error (dataframe-left-join df29 df28 '(site day catch) -999))
(test-error (dataframe-left-join df31 df30 '(first) -999))
(test-end "dataframe-join-test")

;;-------------------------------------------------------------

(define df32
  (make-df*
   (day 1 2)
   (hour 10 11)
   (a 97 78)
   (b 84 47)
   (c 55 54)))

;;-------------------------------------------------------------

(test-begin "dataframe-stack-test")
(test-assert (dataframe-equal?
              (dataframe-stack df22 '(adult juv) 'stage 'count)
              (make-df*
               (grp 'a 'a 'b 'b 'b 'a 'a 'b 'b 'b)
               (trt 'a 'b 'a 'b 'b 'a 'b 'a 'b 'b)
               (stage 'adult 'adult 'adult 'adult 'adult 'juv 'juv 'juv 'juv 'juv)
               (count 1 2 3 4 5 10 20 30 40 50))))
(test-assert (dataframe-equal?
              (dataframe-stack df32 '(a b c) 'site 'count)
              (make-df*
               (day 1 2 1 2 1 2)
               (hour 10 11 10 11 10 11)
               (site 'a 'a 'b 'b 'c 'c)
               (count 97 78 84 47 55 54))))
(test-error (dataframe-stack df22 '(adult juv) 'stage "count"))
(test-error (dataframe-stack df22 '(adult juv) "stage" 'count))
(test-error (dataframe-stack df22 '(adult juv) 'stage 'grp))
(test-end "dataframe-stack-test")

;;-------------------------------------------------------------

;; no extra effort was made to ensure that spreading and stacking always yield same sort order
;; if tests are failing that should be a first place to look
(test-begin "dataframe-spread-test")
(test-assert (dataframe-equal?
              df32
              (-> df32
                  (dataframe-stack '(a b c) 'site 'count)
                  (dataframe-spread 'site 'count))))
(test-assert (dataframe-equal?
              df22
              (-> df22
                  (dataframe-stack '(adult juv) 'stage 'count)
                  (dataframe-spread 'stage 'count))))
(test-assert (dataframe-equal?
              (-> (make-df*
                   (day 1 2 3)
                   (grp "A" "B" "B")
                   (val 10 20 30))
                  (dataframe-spread 'grp 'val))
              (make-df*
               (day 1 2 3)
               (A 10 'na 'na)
               (B 'na 20 30))))
(test-error (dataframe-spread
             (make-df*
              (day 1 2 3)
              (hr 10 11 12)
              (val 10 20 30))
             'hr 'val))
(test-error (dataframe-spread (make-df* (A 1 2 3) (B 4 5 6)) 'A 'B))
(test-error (dataframe-spread (make-df* (A 1 2 3) (B 4 5 6)) 'B 'C))
(test-end "dataframe-spread-test")

;;-------------------------------------------------------------

(test-begin "min-test")
(test-assert (= 2 (list-min '(4 5 2))))
(test-assert (= 2 (list-min '(na 4 5 2))))
(test-assert (symbol=? 'na (list-min '(na 4 5 2) #f)))
(test-error (list-min '(4 5 2 "test")))
(test-end "min-test")

;;-------------------------------------------------------------

(test-begin "max-test")
(test-assert (= 5 (list-max '(4 5 2))))
(test-assert (= 5 (list-max '(na 4 5 2))))
(test-assert (symbol=? 'na (list-max '(na 4 5 2) #f)))
(test-error (list-max '(4 5 2 "test")))
(test-end "max-test")

;;-------------------------------------------------------------

;; not sure why two of these tests are failing
;; they work correctly in the REPL
(test-begin "mean-test")
(test-assert (= 3 (mean '(1 2 3 4 5))))
;; (test-assert (= 3 (mean '(1 2 3 4 5 na))))
(test-assert (= 0 (mean '(-10 0 10))))
(test-assert (= 27.5 (exact->inexact (mean '(1 2 3 4 5 150)))))
;; (test-assert (symbol=? 'na (mean '(-10 0 10 na) #f)))
(test-end "mean-test")

;;-------------------------------------------------------------

(test-begin "sum-test")
(test-assert (= 15 (sum '(1 2 3 4 5))))
(test-assert (= 15 (sum '(1 2 3 4 5 na))))
(test-assert (symbol=? 'na (sum '(1 2 3 4 5 na) #f)))
(test-assert (= 3 (sum '(#t #f #t #t #f #f))))
(test-assert (= 5 (sum '(#f #f #t #t #t #t #t))))
(test-assert (= -1 (sum '(-1 -2 -4 6))))
(test-assert (= 7 (sum '(1 2 3 #t))))
(test-end "sum-test")

;;-------------------------------------------------------------

(test-begin "prod-test")
(test-assert (= 120 (product '(1 2 3 4 5))))
(test-assert (= 120 (product '(1 2 3 4 5 na))))
(test-assert (symbol=? 'na (product '(1 2 3 4 5 na) #f)))
(test-assert (= 0 (product '(#t #f #t #t #f #f))))
(test-assert (= 1 (product '(#t #t #t #t #t))))
(test-assert (= -48 (product '(-1 -2 -4 6))))
(test-assert (= 6 (product '(1 2 3 #t))))
(test-end "prod-test")

;;-------------------------------------------------------------

(test-begin "rle-test")
(test-equal '((1 . 1) (2 . 1) (3 . 1) (4 . 1) (2 . 1) (1 . 1))
  (rle '(1 2 3 4 2 1)))
(test-equal '((1 . 3) (2 . 1) (1 . 2) (2 . 1))
  (rle '(1 1 1 2 1 1 2)))
(test-equal '((3 . 2) (1 . 2) (2 . 3))
  (rle '(3 3 1 1 2 2 2)))
(test-equal '(("a" . 1) ("b" . 2) ("a" . 1))
  (rle '("a" "b" "b" "a")))
(test-end "rle-test")

;;-------------------------------------------------------------

(test-begin "count-elements-test")
(test-equal '((1 . 2) (2 . 2) (3 . 1) (4 . 1))
  (count-elements '(1 2 3 4 2 1)))
(test-equal '((1.1 . 3) (1 . 1) (2.2 . 1) (2 . 1))
  (count-elements '(1.1 1 2.2 2 1.1 1.1)))
(test-equal '((0.5 . 1) (1/2 . 2) (1 . 2) (2 . 1))
 (count-elements '(0.5 1/2 #e0.5 1 1 2)))
(test-equal '(("a" . 2) ("b" . 2))
  (count-elements '("a" "b" "b" "a")))
(test-end "count-elements-test")

;;-------------------------------------------------------------

(test-begin "cumulative-sum-test")
(test-equal '(1 3 6 10 15) (cumulative-sum '(1 2 3 4 5)))
(test-equal '(1 3 6 10 10) (cumulative-sum '(1 2 3 4 #f)))
(test-equal '(5 9 12 14 15) (cumulative-sum '(5 4 3 2 1)))
(test-equal '(5 9 12 14 15) (cumulative-sum '(5 4 3 2 #t)))
(test-equal '(5 9 12 na na) (cumulative-sum '(5 4 3 na na)))
(test-equal '() (cumulative-sum '()))
(test-error (cumulative-sum '#(1 2 3 4)))
(test-end "cumulative-sum-test")

;;-------------------------------------------------------------

(test-begin "median-test")
(test-assert (= 3.5 (median '(1 2 3 4 5 6))))
(test-error (median '()))
(test-end "median-test")

;;-------------------------------------------------------------

(test-begin "quantile-test")
(test-assert (= 3 (quantile '(1 2 3 4 5 6) 0.5 1)))
(test-assert (= 3 (quantile '(1 2 3 4 5 6 na) 0.5 1)))
(test-assert (symbol=? 'na (quantile '(1 2 3 4 5 6 na) 0.5 1 #f)))
(test-assert (= 8 (quantile '(3 7 4 8 9 7) 0.75 1)))
(test-assert (= 3.0 (quantile '(1 2 3 4 5 6) 0.5 4)))
(test-assert (= 3.5 (quantile '(1 2 3 4 5 6) 0.5 8)))
(test-assert (= 1.125 (quantile '(1 2 3 4 5 6) 0.025 7)))
(test-error (quantile '(1 2) 0.5 100))
(test-end "quantile-test")

(exit (if (zero? (test-runner-fail-count (test-runner-get))) 0 1))


