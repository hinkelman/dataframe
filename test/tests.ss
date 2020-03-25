(import (dataframe df)
	(srfi s64 testing))

;; dataframe

(define df1 (make-dataframe '((a 1 2 3) (b 4 5 6))))
(define df2 (make-dataframe '((a 1 2 3) (b 4 5 6) (c 7 8 9))))

(test-begin "dataframe?-test")
(test-assert (dataframe? df1))
(test-assert (not (dataframe? '((a 1 2 3) (b 4 5 6)))))
(test-end "dataframe?-test")

(test-begin "dataframe-alist-test")
(test-equal '((a 1 2 3) (b 4 5 6)) (dataframe-alist df1))
(test-error (dataframe-alist '((a 1 2 3) (b 4 5 6))))
(test-end "dataframe-alist-test")

(define df3 (make-dataframe '((a 1 2 3 1 2 3) (b 4 5 6 4 5 6) (c -999 -999 -999 7 8 9))))
(define df4 (make-dataframe '((a 1 2 3 1 2 3) (b 4 5 6 4 5 6) (c 7 8 9 -999 -999 -999))))

(test-begin "dataframe-append-all-test")
(test-assert (dataframe-equal? df3 (dataframe-append-all -999 df1 df2)))
(test-assert (dataframe-equal? df4 (dataframe-append-all -999 df2 df1)))
(test-error (dataframe-append-all -999 df3 df4 '(1 2 3)))
(test-end "dataframe-append-all-test")

(test-begin "dataframe-contains?-test")
(test-assert (dataframe-contains? df1 'b))
(test-assert (dataframe-contains? df1 'a 'b))
(test-assert (not (dataframe-contains? df1 'a 'b 'c)))
(test-assert (not (dataframe-contains? df1 "b")))
(test-error (dataframe-contains? df1 b))
(test-end "dataframe-contains?-test")

(test-begin "dataframe-dim-test")
(test-equal '(3 . 2) (dataframe-dim df1))
(test-error (dataframe-dim '(1 2 3)))
(test-end "dataframe-dim-test")

(define df5 (make-dataframe '((a 1 2 3) (b 4 5 6) (c 7 8 9))))
(define df6 (make-dataframe '((b 4 5 6) (c 7 8 9))))
(define df7 (make-dataframe '((c 7 8 9))))

(test-begin "dataframe-drop-test")
(test-error (dataframe-drop df5 'd))
(test-assert (dataframe-equal? df6 (dataframe-drop df2 'a)))
(test-assert (dataframe-equal? df7 (dataframe-drop df2 'a 'b)))
(test-end "dataframe-drop-test")

(test-begin "dataframe-equal?-test")
(test-assert (dataframe-equal? df2))
(test-assert (dataframe-equal? df2 df5))
(test-assert (not (dataframe-equal? df5 df6)))
(test-error (dataframes-equal df2 '((a 1 2 3))))
(test-end "dataframe-equal?-test")

(test-begin "dataframe-head-test")
(test-assert (dataframe-equal? df2 (dataframe-head df4 3)))
(test-error (dataframe-head '(1 2 3) 3))
(test-error (dataframe-head df2 10))
(test-error (dataframe-head df2 0.5))
(test-end "dataframe-head-test")

(test-begin "dataframe-tail-test")
(test-assert (dataframe-equal? (make-dataframe '((b 5 6) (c 8 9))) (dataframe-tail df6 1)))
(test-error (dataframe-tail '(1 2 3) 3))
(test-error (dataframe-tail df2 10))
(test-error (dataframe-tail df2 0.5))
(test-end "dataframe-tail-test")

(test-begin "dataframe-names-test")
(test-equal '(a b c) (dataframe-names df5))
(test-equal '(b c) (dataframe-names df6))
(test-equal '(c) (dataframe-names df7))
(test-end "dataframe-names-test")

(define df8 (make-dataframe '((a 1 2 3) (b 4 5 6) (scary7 7 8 9))))
(define df9 (make-dataframe '((A 1 2 3) (b 4 5 6) (scary7 7 8 9))))

(test-begin "dataframe-rename-test")
(test-assert (dataframe-equal? df8 (dataframe-rename df5 '((c scary7)))))
(test-assert (dataframe-equal? df9 (dataframe-rename df5 '((a A) (c scary7)))))
;; same dataframe is returned when old names are not found
(test-assert (dataframe-equal? df8 (dataframe-rename df8 '((d D)))))
(test-error (dataframe-rename 100 '((a A))))
(test-error (dataframe-rename df5 '((a c))))
(test-end "dataframe-rename-test")

(test-begin "dataframe-names-update-test")
(test-assert (dataframe-equal? df8 (dataframe-names-update df5 '(a b scary7))))
(test-assert (dataframe-equal? df9 (dataframe-names-update df5 '(A b scary7))))
(test-error (dataframe-names-update 100 '(a)))
(test-error (dataframe-names-update df5 '(A B)))
(test-error (dataframe-names-update df5 '(a a b)))
(test-error (dataframe-names-update df5 '("a" b c)))
(test-end "dataframe-names-update-test")

(test-begin "dataframe-select-test")
(test-error (dataframe-select df8 'd))
(test-assert (dataframe-equal? df6 (dataframe-select df2 'b 'c)))
(test-assert (dataframe-equal? df7 (dataframe-select df2 'c)))
(test-end "dataframe-select-test")

(define df10 (make-dataframe '((a 100 200 300) (b 4 5 6) (c 700 800 900))))

;; (test-begin "dataframe-update-test")
;; (test-error (dataframe-update df5 (lambda (x) (* x 100)) 'd))
;; (test-assert (dataframe-equal? df10 (dataframe-update df5 (lambda (x) (* x 100)) 'a 'c)))
;; (test-error (dataframe-update df5 (lambda (x) (* x 100)) 'a 'c 'd))
;; (test-error (dataframe-update df5 "test" 'a))
;; (test-end "dataframe-update-test")

(test-begin "dataframe-values-test")
(test-equal '(100 200 300) (dataframe-values df10 'a))
(test-equal '(4 5 6) (dataframe-values df10 'b))
(test-error (dataframe-values df10 'd))
(test-error (dataframe-values 100 'a))
(test-error (dataframe-values df10 'a 'b))
(test-end "dataframe-values-test")

(test-begin "make-dataframe-test")
(test-error (make-dataframe 100))
(test-error (make-dataframe '()))
(test-error (make-dataframe '(1 2 3)))
(test-error (make-dataframe '((a (1 2 3))))) 
(test-error (make-dataframe '(("a" 1 2 3))))
(test-error (make-dataframe '((a 1 2 3) (a 1 2 3))))
(test-error (make-dataframe '((a 1 2 3) (b 1 2 3 4))))
(test-end "make-dataframe-test")

(define df11 (make-dataframe '((a 100 200 300)
                               (b 4 5 6)
                               (c 700 800 900)
                               (d 104 205 306))))
(define df12 (make-dataframe '((a 100 200 300)
                               (b 4 5 6)
                               (c 700 800 900)
                               (d 400 500 600))))
(define df13 (make-dataframe '((a 100 200 300)
                               (b 4 5 6)
                               (c 700 800 900)
                               (d "100_4" "200_5" "300_6"))))
(define df14 (make-dataframe '((a 200 300)
                               (b 5 6)
                               (c 800 900))))
(define df15 (make-dataframe '((a 200)
                               (b 5)
                               (c 800))))

(test-begin "dataframe-filter-test")
(test-assert (dataframe-equal? df14 (dataframe-filter df10 (filter-expr (a) (> a 100)))))
(test-assert (dataframe-equal? df15 (dataframe-filter df10 (filter-expr (b) (= b 5)))))
(test-assert (dataframe-equal? df15 (dataframe-filter df10 (filter-expr (a b) (or (odd? a) (odd? b))))))
(test-end "dataframe-filter-test")

(define df16 (make-dataframe '((a 200)
                               (b 5)
                               (c 800))))
(define df17 (make-dataframe '((a 100 300)
                               (b 4 6)
                               (c 700 900))))

(test-begin "dataframe-partition-test")
(define-values (part1 part2) (dataframe-partition df10 (filter-expr (b) (odd? b))))
(test-assert (dataframe-equal? part1 df16))
(test-assert (dataframe-equal? part2 df17))
(test-end "dataframe-partition-test")

(test-begin "df-read-write-test")
(dataframe-write df10 "example.scm" #f)
(test-error (dataframe-write df10 "example.scm" #f))
(test-assert (dataframe-equal? df10 (dataframe-read "example.scm")))
(delete-file "example.scm")
(test-end "df-read-write-test")

(define df18 (make-dataframe '((a 1 2 3 1 2 3)
                               (b 4 5 6 4 5 6))))
(define df19 (make-dataframe '((c 1 2 3))))

(test-begin "dataframe-append-test")
(test-assert (dataframe-equal? df18 (dataframe-append df1 df2)))
(test-assert (dataframe-equal? df18 (dataframe-append df2 df1)))
(test-error (dataframe-append df1 df19))
(test-error (dataframe-append-all df3 df4 '(1 2 3)))
(test-end "dataframe-append-test")

(test-begin "dataframe->rowtable-test")
(test-error (dataframe->rowtable 100))
(test-equal '((a b c) (100 4 700) (300 6 900)) (dataframe->rowtable df17))
(test-equal '((c) (1) (2) (3)) (dataframe->rowtable df19))
(test-end "dataframe->rowtable-test")

(test-begin "rowtable->dataframe-test")
(test-assert (dataframe-equal? df2 (rowtable->dataframe '((a b c) (1 4 7) (2 5 8) (3 6 9)) #t)))
(test-assert (dataframe-equal? df17 (rowtable->dataframe '((a b c) (100 4 700) (300 6 900)) #t)))
(test-assert (dataframe-equal? df19 (rowtable->dataframe '((c) (1) (2) (3)) #t)))
(test-assert (dataframe-equal? (make-dataframe '((V0 1 2) (V1 3 4)))
                               (rowtable->dataframe '((1 3) (2 4)) #f)))
(test-error (rowtable->dataframe '(("a" b c) (1 4 7) (2 5 8) (3 6 9)) #t))
(test-error (rowtable->dataframe '((a b) (1 4 7))))
(test-end "rowtable->dataframe-test")

(define df20 (make-dataframe '((trt A A A B B B)
                               (grp A B A B A B)
                               (resp 1 2 3 4 5 6))))
(define df21 (make-dataframe '((trt A A B B)
                               (grp B A A B))))

(test-begin "dataframe-unique-test")
(test-error (dataframe-unique '((a (1 2 3)))))
(test-assert (dataframe-equal? df21 (dataframe-unique (dataframe-select df20 'trt 'grp))))
(test-end "dataframe-unique-test")

(define df22 (make-dataframe '((grp a a b b b)
                               (trt a b a b b)
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))

(test-begin "dataframe-split-test")
(define df-list (dataframe-split df22 'grp))
(test-assert (dataframe-equal? (car df-list)
                               (dataframe-filter df22 (filter-expr (grp) (symbol=? grp 'a)))))
(test-assert (dataframe-equal? (cadr df-list)
                               (dataframe-filter df22 (filter-expr (grp) (symbol=? grp 'b)))))
(test-assert (dataframe-equal? df22 (apply dataframe-append df-list)))
(test-end "dataframe-split-test")


(define df23 (make-dataframe '((grp a a b b b)
                               (trt a b a b b)
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50)
                               (total 11 22 33 44 55))))
(define df24 (make-dataframe '((grp a a b b b)
                               (trt a b a b b)
                               (adult 1 2 3 4 5)
                               (juv 5 10 15 20 25))))
(define df25 (make-dataframe '((grp a b)
                               (trt b b)
                               (adult 2 5)
                               (juv 20 50)
                               (juv-mean 15 40))))
(define df26 (make-dataframe '((grp a a b b b)
                               (trt a b a b b)
                               (adult 1 2 3 4 5)
                               (juv 5 10 15 20 25)
                               (total 11 22 33 44 55))))

(test-begin "dataframe-modify-test")
(test-assert (dataframe-equal? df23 (dataframe-modify
                                     df22
                                     (modify-expr (total (adult juv) (+ adult juv))))))
(test-assert (dataframe-equal? df24 (dataframe-modify
                                     df22
                                     (modify-expr (juv (juv) (/ juv 2))))))
(test-error (dataframe-modify df22 (modify-expr ("test" (juv) (/ juv 2)))))
(test-end "dataframe-modify-test")

(test-begin "thread-test")
;; (test-equal 12 (-> '(1 2 3) (mean) (+ 10)))
;; (test-approximate 0 (-> (random-binomial 1e5 10 0.5) (variance) (- 2.5)) 0.125)
(test-assert (dataframe-equal? df1
                               (-> df1
                                   (dataframe-modify
                                    (modify-expr (c () '(7 8 9))))
                                   (dataframe-drop 'c))))
(test-assert (dataframe-equal? df23
                               (-> '((grp a a b b b)
                                     (trt a b a b b)
                                     (adult 1 2 3 4 5)
                                     (juv 10 20 30 40 50))
                                   (make-dataframe)
                                   (dataframe-modify
                                    (modify-expr (total (adult juv) (+ adult juv)))))))
(test-assert (dataframe-equal? df24
                               (-> '((grp a a b b b)
                                     (trt a b a b b)
                                     (adult 1 2 3 4 5)
                                     (juv 10 20 30 40 50))
                                   (make-dataframe)
                                   (dataframe-modify
                                    (modify-expr (juv (juv) (/ juv 2)))))))
(test-assert (dataframe-equal? df26
                               (-> df22
                                   (dataframe-modify
                                    (modify-expr (juv (juv) (/ juv 2))
                                                 (total (adult juv) (+ adult juv)))))))
(test-assert (dataframe-equal? df23
                               (-> df22
                                   (dataframe-split 'grp)
                                   (dataframe-list-modify
                                    (modify-expr (total (adult juv) (+ adult juv)))))))
;; (test-assert (dataframe-equal? df23   
;;                                (-> df22
;;                                    (dataframe-split 'grp)
;;                                    (->> (map (lambda (df)
;;                                                (dataframe-modify
;;                                                 df
;;                                                 (modify-expr (total (adult juv) (+ adult juv)))))))
;;                                    (->> (apply dataframe-append)))))
;; (test-assert (dataframe-equal? df25
;;                                (-> df22
;;                                    (dataframe-split 'grp)
;;                                    (->> (map (lambda (df)
;;                                                (dataframe-modify
;;                                                 df
;;                                                 (modify-expr (juv-mean () (mean ($ df 'juv))))))))
;;                                    (->> (apply dataframe-append))
;;                                    (dataframe-filter (filter-expr (juv juv-mean) (> juv juv-mean))))))
;; can't use dataframe-list-modify shorthand when performing calculation (e.g., mean) on a column
;; because df is undefined; need long form with explicit lambda
;; (test-assert (dataframe-equal? df25
;;                                (-> df22
;;                                    (dataframe-split 'grp)
;;                                    (dataframe-list-modify
;;                                     (modify-expr (juv-mean () (mean ($ df 'juv)))))
;;                                    (dataframe-filter (filter-expr (juv juv-mean) (> juv juv-mean))))))
;; (test-error (-> '(4 3 5 1) (sort <)))
(test-end "thread-test")

(test-begin "dataframe-aggregate-test")
(test-assert (dataframe-equal?
              (make-dataframe '((grp a b)
                                (adult-sum 3 12)
                                (juv-sum 30 120)))
              (dataframe-aggregate df22 '(grp)
                                   (aggregate-expr (adult-sum (adult) (apply + adult))
                                                   (juv-sum (juv) (apply + juv))))))
(test-assert (dataframe-equal?
              (make-dataframe '((grp a a b b)
                                (trt a b a b)
                                (adult-sum 1 2 3 9)
                                (juv-sum 10 20 30 90)))
              (dataframe-aggregate df22 '(grp trt)
                                   (aggregate-expr (adult-sum (adult) (apply + adult))
                                                   (juv-sum (juv) (apply + juv))))))
(test-end "dataframe-aggregate-test")

(define df27 (make-dataframe '((grp "a" "a" "b" "b" "b")
                               (trt "a" "b" "a" "b" "b")
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))

(test-begin "dataframe-sort-test")
(test-assert (dataframe-equal?
              (make-dataframe '((grp "a" "b" "b" "a" "b")
                                (trt "b" "b" "b" "a" "a")
                                (adult 2 4 5 1 3)
                                (juv 20 40 50 10 30)))
              (dataframe-sort df27 (sort-expr (string<? trt)))))
(test-assert (dataframe-equal?
              (make-dataframe '((grp "b" "b" "a" "b" "a")
                                (trt "b" "b" "b" "a" "a")
                                (adult 5 4 2 3 1)
                                (juv 50 40 20 30 10)))
              (dataframe-sort df27 (sort-expr (string<? trt) (< adult)))))
(test-end "dataframe-sort-test")

(test-begin "dataframe-ref-test")
(test-assert (dataframe-equal? (make-dataframe '((grp "a" "b" "b") (trt "a" "a" "b") (adult 1 3 5) (juv 10 30 50)))
                               (dataframe-ref df27 '(0 2 4))))
(test-assert (dataframe-equal? (make-dataframe '((adult 1 3 5) (juv 10 30 50)))
                               (dataframe-ref df27 '(0 2 4) 'adult 'juv)))
(test-error (dataframe-ref df27 '()))
(test-error (dataframe-ref df27 '(0 10)))
(test-error (dataframe-ref df27 2))
(test-error (dataframe-ref df27 '(2) 'total))
(test-end "dataframe-ref-test")

(exit (if (zero? (test-runner-fail-count (test-runner-get))) 0 1))

