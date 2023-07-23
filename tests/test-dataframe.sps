#!/usr/bin/env scheme-script
;; -*- mode: scheme; coding: utf-8 -*- !#
;; Copyright (c) 2020 Travis Hinkelman
;; SPDX-License-Identifier: MIT
#!r6rs

(import (srfi :64 testing)
        (dataframe))

(define df1
  (make-df*
   (a 1 2 3)
   (b 4 5 6)))

(define df2
  (make-df*
   (a 1 2 3)
   (b 4 5 6)
   (c 7 8 9)))

;;-------------------------------------------------------------

(test-begin "dataframe?-test")
(test-assert (dataframe? df1))
(test-assert (not (dataframe? '((a 1 2 3) (b 4 5 6)))))
(test-end "dataframe?-test")

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
              (dataframe-bind 'na df1 df2)))
(test-assert (dataframe-equal?
              df4
              (dataframe-bind 'na df2 df1)))
(test-error (dataframe-bind 'na df3 df4 '(1 2 3)))
(test-end "dataframe-bind-test")

;;-------------------------------------------------------------

(test-begin "dataframe-contains?-test")
(test-assert (dataframe-contains? df1 'b))
(test-assert (dataframe-contains? df1 'a 'b))
(test-assert (not (dataframe-contains? df1 'a 'b 'c)))
(test-assert (not (dataframe-contains? df1 "b")))
(test-error (dataframe-contains? df1 b))
(test-end "dataframe-contains?-test")

;;-------------------------------------------------------------

(test-begin "dataframe-dim-test")
(test-equal (cons 3 2) (dataframe-dim df1))
(test-error (dataframe-dim '(1 2 3)))
(test-end "dataframe-dim-test")

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
(test-error (dataframe-drop df5 'd))
(test-assert (dataframe-equal? df6 (dataframe-drop* df2 a)))
(test-assert (dataframe-equal? df7 (dataframe-drop* df2 a b)))
(test-end "dataframe-drop-test")

;;-------------------------------------------------------------

(test-begin "dataframe-equal?-test")
(test-assert (dataframe-equal? df2))
(test-assert (dataframe-equal? df2 df5))
(test-assert (not (dataframe-equal? df5 df6)))
(test-error (dataframes-equal df2 '((a 1 2 3))))
(test-end "dataframe-equal?-test")

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
(test-assert (dataframe-equal? (make-df* (b 5 6) (c 8 9))
                               (dataframe-tail df6 1)))
(test-error (dataframe-tail '(1 2 3) 3))
(test-error (dataframe-tail df2 3))
(test-error (dataframe-tail df2 0.5))
(test-error (dataframe-tail df2 -1))
(test-end "dataframe-tail-test")

;;-------------------------------------------------------------

(test-begin "dataframe-names-test")
(test-equal '(a b c) (dataframe-names df5))
(test-equal '(b c) (dataframe-names df6))
(test-equal '(c) (dataframe-names df7))
(test-end "dataframe-names-test")

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
(test-assert (dataframe-equal? df9 (dataframe-rename* df5 (a A) (c scary7))))
;; same dataframe is returned when old names are not found
(test-assert (dataframe-equal? df8 (dataframe-rename* df8 (d D))))
(test-error (dataframe-rename* 100 (a A)))
(test-error (dataframe-rename* df5 (a c)))
(test-end "dataframe-rename-test")

;;-------------------------------------------------------------

(test-begin "dataframe-rename-all-test")
(test-assert (dataframe-equal? df8 (dataframe-rename-all df5 '(a b scary7))))
(test-assert (dataframe-equal? df9 (dataframe-rename-all df5 '(A b scary7))))
(test-error (dataframe-rename-all 100 '(a)))
(test-error (dataframe-rename-all df5 '(A B)))
(test-error (dataframe-rename-all df5 '(a a b)))
(test-error (dataframe-rename-all df5 '("a" b c)))
(test-end "dataframe-rename-all-test")

;;-------------------------------------------------------------

(test-begin "dataframe-select-test")
(test-error (dataframe-select df8 'd))
(test-assert (dataframe-equal? df6 (dataframe-select* df2 b c)))
(test-assert (dataframe-equal? df7 (dataframe-select* df2 c)))
(test-end "dataframe-select-test")

;;-------------------------------------------------------------

(define df10
  (make-df*
   (a 100 200 300)
   (b 4 5 6)
   (c 700 800 900)))

;;-------------------------------------------------------------

;; (test-begin "dataframe-modify-at-all-test")
;; (test-error (dataframe-modify-at df5 (lambda (x) (* x 100)) 'd))
;; (test-assert (dataframe-equal?
;;               df10
;;               (dataframe-modify-at df5 (lambda (x) (* x 100)) 'a 'c)))
;; (test-assert (dataframe-equal?
;;               (make-dataframe
;;                '((a 100 200 300)
;;                  (b 400 500 600)
;;                  (c 700 800 900)))
;;               (dataframe-modify-all df5 (lambda (x) (* x 100)))))
;; (test-error (dataframe-modify-at df5 (lambda (x) (* x 100)) 'a 'c 'd))
;; (test-error (dataframe-modify-at df5 "test" 'a))
;; (test-end "dataframe-modify-at-all-test")

;;-------------------------------------------------------------

(test-begin "dataframe-values-test")
(test-equal '(100 200 300) (dataframe-values df10 'a))
(test-equal '(4 5 6) (dataframe-values df10 'b))
(test-error (dataframe-values df10 'd))
(test-error (dataframe-values 100 'a))
(test-error (dataframe-values df10 'a 'b))
(test-end "dataframe-values-test")

;;-------------------------------------------------------------

;; (test-begin "dataframe-values-unique-test")
;; (test-equal '(a b c) (dataframe-values-unique
;;                       (make-dataframe '((x a a b b c))) 'x))
;; (test-equal '(a b c) (dataframe-values-unique
;;                       (make-dataframe '((x a b c))) 'x))
;; (test-equal '(a) (dataframe-values-unique
;;                   (make-dataframe '((x a a) (y a b))) 'x))
;; (test-error (dataframe-values-unique '((x a a) (y a b)) 'z))
;; (test-end "dataframe-values-unique-test")


;;-------------------------------------------------------------

(test-begin "make-series-test")
(test-error (make-series 'a '()))
(test-error (make-series 'a 42))
(test-error (make-series "a" '(1 2 3)))
(test-end "make-series-test")

;;-------------------------------------------------------------

(test-begin "make-dataframe-test")
(test-error (make-dataframe '()))
(test-error (make-dataframe (list '(1 2 3)
                                  (make-series* (a 1 2 3)))))
(test-error (make-dataframe (list (make-series* (a 1 2 3))
                                  (make-series* (a 1 2 3)))))
(test-error (make-dataframe (list (make-series* (a 1 2 3))
                                  (make-series* (b 1 2 3 4))))) 
(test-end "make-dataframe-test")

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

;; (test-begin "dataframe-filter-test")
;; (test-assert (dataframe-equal?
;;               df14
;;               (dataframe-filter df10 '(a) (lambda (a) (> a 100)))))
;; (test-assert (dataframe-equal?
;;               df15
;;               (dataframe-filter df10 '(b) (lambda (b) (= b 5)))))
;; (test-assert (dataframe-equal?
;;               df15
;;               (dataframe-filter df10 '(a b) (lambda (a b) (or (odd? a) (odd? b))))))
;; (test-assert (dataframe-equal? df14 (dataframe-filter* df10 (a) (> a 100))))
;; (test-assert (dataframe-equal? df15 (dataframe-filter* df10 (b) (= b 5))))
;; (test-assert (dataframe-equal?
;;               df15
;;               (dataframe-filter* df10 (a b) (or (odd? a) (odd? b)))))
;; (test-assert (dataframe-equal?
;;               (make-dataframe
;;                '((a 100 300)
;;                  (b 4 6)
;;                  (c 700 900)))
;;               (dataframe-filter-all df10 even?)))
;; (test-assert (dataframe-equal? df10 (dataframe-filter-at df10 even? 'a 'c)))
;; (test-error (dataframe-filter-all df10 odd?))
;; (test-end "dataframe-filter-test")

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

;;-------------------------------------------------------------

;; (test-begin "dataframe-partition-test")
;; (define-values (part1 part2) (dataframe-partition* df10 (b) (odd? b)))
;; (test-assert (dataframe-equal? part1 df16))
;; (test-assert (dataframe-equal? part2 df17))
;; (test-end "dataframe-partition-test")

;;-------------------------------------------------------------

;; (test-begin "df-read-write-test")
;; (dataframe-write df10 "example.scm" #f)
;; (test-error (dataframe-write df10 "example.scm" #f))
;; (test-assert (dataframe-equal? df10 (dataframe-read "example.scm")))
;; (delete-file "example.scm")
;; (test-end "df-read-write-test")

;;-------------------------------------------------------------

(define df18
  (make-df*
   (a 1 2 3 1 2 3)
   (b 4 5 6 4 5 6)))
(define df19
  (make-df*
   (c 1 2 3)))

;;-------------------------------------------------------------

;; (test-begin "dataframe-bind-test")
;; (test-assert (dataframe-equal? df18 (dataframe-bind df1 df2)))
;; (test-assert (dataframe-equal? df18 (dataframe-bind df2 df1)))
;; (test-error (dataframe-bind df1 df19))
;; (test-error (dataframe-bind-all df3 df4 '(1 2 3)))
;; (test-end "dataframe-bind-test")

;;-------------------------------------------------------------

;; (test-begin "dataframe->rowtable-test")
;; (test-error (dataframe->rowtable 100))
;; (test-equal '((a b c) (100 4 700) (300 6 900)) (dataframe->rowtable df17))
;; (test-equal '((c) (1) (2) (3)) (dataframe->rowtable df19))
;; (test-end "dataframe->rowtable-test")

;;-------------------------------------------------------------

;; (test-begin "rowtable->dataframe-test")
;; (test-assert (dataframe-equal?
;;               df2
;;               (rowtable->dataframe
;;                '((a b c)
;;                  (1 4 7)
;;                  (2 5 8)
;;                  (3 6 9)))))
;; (test-assert (dataframe-equal?
;;               df2
;;               (rowtable->dataframe
;;                '((a b c)
;;                  ("1" "4" "7")
;;                  (2 5 8)
;;                  ("3" "6" "9")))))
;; (test-assert (dataframe-equal?
;;               (make-dataframe
;;                '((a 1 2 3)
;; 		 (b 4 5 6)
;; 		 (c "7" "8" "NA")))
;;               (rowtable->dataframe
;;                '((a b c)
;;                  (1 4 "7")
;;                  (2 5 "8")
;;                  (3 6 "NA"))
;;                #t #f)))
;; ;; this is same as above but it fails the try check
;; (test-assert (dataframe-equal?
;;               (make-dataframe
;;                '((a 1 2 3)
;; 		 (b 4 5 6)
;; 		 (c "7" "8" "NA")))
;;               (rowtable->dataframe
;;                '((a b c)
;;                  (1 4 "7")
;;                  (2 5 "8")
;;                  (3 6 "NA"))
;;                #t #t)))
;; (test-assert (dataframe-equal?
;;               (make-dataframe
;;                '((a 1 2 3)
;; 		 (b 4 5 6)
;; 		 (c 7 8 #f)))
;;               (rowtable->dataframe
;;                '((a b c)
;;                  ("1" 4 "7")
;;                  ("2" 5 "8")
;;                  ("3" 6 "NA"))
;;                #t #t 2)))
;; (test-assert (dataframe-equal?
;;               df17
;;               (rowtable->dataframe
;;                '((a b c)
;;                  (100 4 700)
;;                  (300 6 900)))))
;; (test-assert (dataframe-equal?
;;               df19
;;               (rowtable->dataframe
;;                '((c)
;;                  (1)
;;                  (2)
;;                  (3)))))
;; (test-assert (dataframe-equal?
;;               (make-dataframe
;;                '((V0 1 2)
;;                  (V1 3 4)))
;;               (rowtable->dataframe
;;                '((1 3)
;;                  (2 4))
;;                #f)))
;; (test-assert (dataframe-equal?
;;               df2
;; 	      (rowtable->dataframe
;;                '(("a" b c)
;; 		 (1 4 7)
;; 		 (2 5 8)
;; 		 (3 6 9)))))
;; (test-error (rowtable->dataframe '((a b) (1 4 7))))
;; (test-error (rowtable->dataframe '((a b) (1 4)) 1))
;; (test-error (rowtable->dataframe '((a b) (1 4)) #t 1))
;; (test-error (rowtable->dataframe '((a b) (1 4)) #t #t 0))
;; (test-end "rowtable->dataframe-test")

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

;; (test-begin "dataframe-split-test")
;; (define df-list (dataframe-split df22 'grp))
;; (test-assert (dataframe-equal?
;;               (car df-list)
;;               (dataframe-filter* df22 (grp) (symbol=? grp 'a))))
;; (test-assert (dataframe-equal?
;;               (cadr df-list)
;;               (dataframe-filter* df22 (grp) (symbol=? grp 'b))))
;; (test-assert (dataframe-equal? df22 (apply dataframe-bind df-list)))
;; (test-end "dataframe-split-test")

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

;; (test-begin "dataframe-modify-test")
;; (test-assert (dataframe-equal?
;;               df23
;;               (dataframe-modify*
;;                df22
;;                (total (adult juv) (+ adult juv)))))
;; (test-assert (dataframe-equal?
;;               df23
;;               (dataframe-modify
;;                df22
;;                '(total)
;;                '((adult juv))
;;                (lambda (adult juv) (+ adult juv)))))
;; (test-assert (dataframe-equal?
;;               df24
;;               (dataframe-modify*
;;                df22
;;                (juv (juv) (/ juv 2)))))
;; (test-assert (dataframe-equal?
;;               df24
;;               (dataframe-modify
;;                df22
;;                '(juv)
;;                '((juv))
;;                (lambda (juv) (/ juv 2)))))
;; (test-error (dataframe-modify* df22 ("test" (juv) (/ juv 2))))
;; (test-end "dataframe-modify-test")

;;-------------------------------------------------------------

;; (test-begin "thread-test")
;; (define (mean ls) (/ (apply + ls) (length ls)))
;; (test-equal 12 (-> '(1 2 3) (mean) (+ 10)))
;; (test-assert (dataframe-equal?
;;               df1
;;               (-> df1
;;                   (dataframe-modify*
;;                    (c () '(7 8 9)))
;;                   (dataframe-drop 'c))))
;; (test-assert (dataframe-equal?
;;               df23
;;               (-> '((grp a a b b b)
;;                     (trt a b a b b)
;;                     (adult 1 2 3 4 5)
;;                     (juv 10 20 30 40 50))
;;                   (make-dataframe)
;;                   (dataframe-modify*
;;                    (total (adult juv) (+ adult juv))))))
;; (test-assert (dataframe-equal?
;;               df24
;;               (-> '((grp a a b b b)
;;                     (trt a b a b b)
;;                     (adult 1 2 3 4 5)
;;                     (juv 10 20 30 40 50))
;;                   (make-dataframe)
;;                   (dataframe-modify*
;;                    (juv (juv) (/ juv 2))))))
;; (test-assert (dataframe-equal?
;;               df26
;;               (-> df22
;;                   (dataframe-modify*
;;                    (juv (juv) (/ juv 2))
;;                    (total (adult juv) (+ adult juv))))))
;; (test-assert (dataframe-equal?
;;               df23   
;;               (-> df22
;;                   (dataframe-split 'grp)
;;                   (->> (map (lambda (df)
;;                               (dataframe-modify*
;;                                df
;;                                (total (adult juv) (+ adult juv))))))
;;                   (->> (apply dataframe-bind)))))
;; (test-assert (dataframe-equal?
;;               df25
;;               (-> df22
;;                   (dataframe-split 'grp)
;;                   (->> (map (lambda (df)
;;                               (dataframe-modify*
;;                                df
;;                                (juv-mean () (mean ($ df 'juv)))))))
;;                   (->> (apply dataframe-bind))
;;                   (dataframe-filter* (juv juv-mean) (> juv juv-mean)))))
;; (test-error (-> df22
;;                 (dataframe-filter* (adult) (< adult 1))
;;                 (dataframe-sort* (> juv))))
;; (test-error (-> '(4 3 5 1) (sort <)))
;; (test-end "thread-test")

;;-------------------------------------------------------------

;; (test-begin "dataframe-aggregate-test")
;; (test-assert (dataframe-equal?
;;               (make-dataframe
;;                '((grp a b)
;;                  (adult-sum 3 12)
;;                  (juv-sum 30 120)))
;;               (dataframe-aggregate*
;;                df22
;;                (grp)
;;                (adult-sum (adult) (apply + adult))
;;                (juv-sum (juv) (apply + juv)))))
;; (test-assert (dataframe-equal?
;;               (make-dataframe
;;                '((grp a b)
;;                  (adult-sum 3 12)
;;                  (juv-sum 30 120)))
;;               (dataframe-aggregate
;;                df22
;;                '(grp)
;;                '(adult-sum juv-sum)
;;                '((adult) (juv))
;;                (lambda (adult) (apply + adult))
;;                (lambda (juv) (apply + juv)))))
;; (test-assert (dataframe-equal?
;;               (make-dataframe
;;                '((grp a a b b)
;;                  (trt a b a b)
;;                  (adult-sum 1 2 3 9)
;;                  (juv-sum 10 20 30 90)))
;;               (dataframe-aggregate*
;;                df22
;;                (grp trt)
;;                (adult-sum (adult) (apply + adult))
;;                (juv-sum (juv) (apply + juv)))))
;; (test-assert (dataframe-equal?
;;               (make-dataframe
;;                '((grp a a b b)
;;                  (trt a b a b)
;;                  (adult-sum 1 2 3 9)
;;                  (juv-sum 10 20 30 90)))
;;               (dataframe-aggregate
;;                df22
;;                '(grp trt)
;;                '(adult-sum juv-sum)
;;                '((adult) (juv))
;;                (lambda (adult) (apply + adult))
;;                (lambda (juv) (apply + juv)))))
;; (test-end "dataframe-aggregate-test")

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

(test-begin "dataframe-ref-test")
(test-assert (dataframe-equal?
              (make-df*
               (grp "a" "b" "b")
               (trt "a" "a" "b")
               (adult 1 3 5)
               (juv 10 30 50))
              (dataframe-ref df27 '(0 2 4))))
(test-assert (dataframe-equal?
              (make-df* (adult 1 3 5) (juv 10 30 50))
              (dataframe-ref df27 '(0 2 4) 'adult 'juv)))
(test-error (dataframe-ref df27 '()))
(test-error (dataframe-ref df27 '(0 10)))
(test-error (dataframe-ref df27 2))
(test-error (dataframe-ref df27 '(2) 'total))
(test-end "dataframe-ref-test")

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

;; (test-begin "dataframe-join-test")
;; (test-assert (dataframe-equal?
;;               (dataframe-left-join df28 df29 '(site) -999)
;;               (make-dataframe
;;                '((site "b" "b" "a" "c" "c")
;;                  (habitat "grassland" "grassland" "meadow" "woodland" "woodland")
;;                  (day 1 2 -999 1 2)
;;                  (catch 12 24 -999 10 20)))))
;; (test-assert (dataframe-equal?
;;               (dataframe-left-join df29 df28 '(site) -999)
;;               (make-dataframe
;;                '((site "c" "c" "b" "b")
;;                  (day 1 2 1 2)
;;                  (catch 10 20 12 24)
;;                  (habitat "woodland" "woodland" "grassland" "grassland")))))
;; (test-error (dataframe-left-join df31 (dataframe-alist df30) '(first last) -999))
;; (test-error (dataframe-left-join df29 df28 '(site day catch) -999))
;; (test-error (dataframe-left-join df31 df30 '(first) -999))
;; (test-end "dataframe-join-test")

;;-------------------------------------------------------------

(define df32
  (make-df*
   (day 1 2)
   (hour 10 11)
   (a 97 78)
   (b 84 47)
   (c 55 54)))

;;-------------------------------------------------------------

;; (test-begin "dataframe-stack-test")
;; (test-assert (dataframe-equal?
;;               (dataframe-stack df22 '(adult juv) 'stage 'count)
;;               (make-dataframe
;;                '((grp a a b b b a a b b b)
;;                  (trt a b a b b a b a b b)
;;                  (stage adult adult adult adult adult juv juv juv juv juv)
;;                  (count 1 2 3 4 5 10 20 30 40 50)))))
;; (test-assert (dataframe-equal?
;;               (dataframe-stack df32 '(a b c) 'site 'count)
;;               (make-dataframe
;;                '((day 1 2 1 2 1 2)
;;                  (hour 10 11 10 11 10 11)
;;                  (site a a b b c c)
;;                  (count 97 78 84 47 55 54)))))
;; (test-error (dataframe-stack '(1 2 3) 'stage 'count '(adult juv)))
;; (test-error (dataframe-stack df22 'grp 'count '(adult juv)))
;; (test-error (dataframe-stack df22 'stage "count" '(adult juv)))
;; (test-error (dataframe-stack df22 'stage 'count '(adult juvenile)))
;; (test-end "dataframe-stack-test")

;;-------------------------------------------------------------

;; no extra effort was made to ensure that spreading and stacking always yield same sort order
;; if tests are failing that should be a first place to look
;; (test-begin "dataframe-spread-test")
;; (test-assert (dataframe-equal?
;;               df32
;;               (-> df32
;;                   (dataframe-stack '(a b c) 'site 'count)
;;                   (dataframe-spread 'site 'count -999))))
;; (test-assert (dataframe-equal?
;;               df22
;;               (-> df22
;;                   (dataframe-stack '(adult juv) 'stage 'count)
;;                   (dataframe-spread 'stage 'count -999))))
;; (test-assert (dataframe-equal?
;;               (-> '((day 1 2 3)
;;                     (grp "A" "B" "B")
;;                     (val 10 20 30))
;;                   (make-dataframe)
;;                   (dataframe-spread 'grp 'val -999))
;;               (make-dataframe
;;                '((day 1 2 3)
;;                  (A 10 -999 -999)
;;                  (B -999 20 30)))))
;; (test-error (dataframe-spread
;;              (make-dataframe
;;               '((day 1 2 3)
;;                 (hr 10 11 12)
;;                 (val 10 20 30)))
;;              'hr 'val -999))
;; (test-error (dataframe-spread (make-dataframe '((A 1 2 3) (B 4 5 6))) 'B 'C -999))
;; (test-end "dataframe-spread-test")

;;-------------------------------------------------------------

(exit (if (zero? (test-runner-fail-count (test-runner-get))) 0 1))


