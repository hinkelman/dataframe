(library (dataframe helpers)
  (export
   list-head
   make-list
   add1
   sub1
   iota
   enumerate
   na?
   ;; add-names-ls-vals
   ;; alist-select
   ;; alist-drop
   ;; alist-ref
   ;; alist-repeat-rows
   ;; alist-values
   ;; alist-values-map
   ;; combine-names-ordered
   ;; filter-ls-vals
   ;; filter-vals
   ;; flatten
   ;; get-all-names
   ;; get-all-unique-names
   ;; cartesian-product
   ;; check-index
   ;; check-integer-gte-zero
   ;; check-integer-positive
   ;; check-list
   ;; check-names-unique
   ;; check-names-symbol
   ;; check-names
   ;; check-names-duplicate
   ;; check-new-names
   ;; check-name-pairs
   ;; check-alist
   ;; not-in
   ;; partition-ls-vals
   ;; rep
   remove-duplicates
   transpose)

  (import (rnrs))

  (define (na? obj)
    (and (symbol? obj)
         (symbol=? obj 'na)))

  (define (flatten x)
    (cond ((null? x) '())
          ((not (pair? x)) (list x))
          (else (append (flatten (car x))
                        (flatten (cdr x))))))

  (define (remove-duplicates lst)
    (let ([ht (make-hashtable equal-hash equal?)])
      (let loop ([lst lst]
                 [results '()])
        (cond [(null? lst)
               (reverse results)]
              [(hashtable-ref ht (car lst) #f)
               (loop (cdr lst) results)] ;; value already in ht
              [else
               ;; value is arbitrarily set to 0; key is what matters
               (hashtable-set! ht (car lst) 0) 
               (loop (cdr lst) (cons (car lst) results))]))))

  (define (transpose lst)
    (apply map list lst))

  (define (not-in xs ys)
    (filter (lambda (x) (not (member x ys))) xs))

  (define (rep lst n type)
    (cond [(symbol=? type 'each)
           (apply append (map (lambda (x) (make-list n x)) lst))]
          [(symbol=? type 'times)
           (rep-times lst n)]
          [else
           (assertion-violation "(rep ls n type)"
                                "type must be 'each or 'times")]))

  (define (rep-times lst n)
    (define (loop out n)
      (if (= n 1) out (loop (append lst out) (sub1 n))))
    (loop lst n))

  (define (cartesian-product . lst)
    (fold-right product-of-two '(()) lst))

  (define (product-of-two lst1 lst2)
    (apply append
           (map (lambda (x)
                  (map (lambda (y)
                         (cons x y))
                       lst2))
                lst1)))

  ;; ls-vals ------------------------------------------------------------------------
  
  ;; add names to list of vals, ls-vals, to create association list
  (define (add-names-ls-vals names ls-vals)
    (if (null? ls-vals)
        (map (lambda (name) (cons name '())) names)
        (map (lambda (name vals) (cons name vals)) names ls-vals)))

  ;; in previous version, would pass over ls-vals twice with filter-ls-vals (with bools negated on 1 pass)
  ;; the extra transposing in this version is faster than two passes with filter-ls-vals
  (define (partition-ls-vals bools ls-vals)
    (let loop ([bools bools]
               [ls-rows (transpose ls-vals)]
               [keep '()]
               [drop '()])
      (if (null? bools)
          (values (transpose (reverse keep)) (transpose (reverse drop)))
          (if (car bools)
              (loop (cdr bools) (cdr ls-rows) (cons (car ls-rows) keep) drop)
              (loop (cdr bools) (cdr ls-rows) keep (cons (car ls-rows) drop))))))
  
  (define (filter-ls-vals bools ls-vals)
    (map (lambda (vals) (filter-vals bools vals)) ls-vals))

  ;; filter vals by list of booleans of same length as vals
  (define (filter-vals bools vals)
    (let ([bools-vals (map cons bools vals)])
      (map cdr (filter (lambda (x) (car x)) bools-vals))))

  ;; alists ------------------------------------------------------------------------

  (define (alist-select alist names)
    (map (lambda (name) (assoc name alist)) names))

  (define (alist-drop alist names)
    (filter (lambda (column) (not (member (car column) names))) alist))

  (define (alist-values alist name)
    (cdr (assoc name alist)))

  (define (alist-values-map alist names)
    (map (lambda (name) (alist-values alist name)) names))

  (define (alist-ref alist indices names)
    (let ([ls-vals (alist-values-map alist names)])
      (add-names-ls-vals
       names
       (map (lambda (vals)
              (map (lambda (n) (list-ref vals n)) indices))
            ls-vals))))

  ;; expand an list by repeating rows n times (or each)
  (define (alist-repeat-rows alist n type)
    (map (lambda (ls) (cons (car ls) (rep (cdr ls) n type))) alist))

  (define (get-all-names . alists)
    (apply append (map (lambda (alist) (map car alist)) alists)))

  (define (get-all-unique-names . alists)
    (remove-duplicates (apply get-all-names alists)))

  ;; combine names such that they stay in the order that they appear in each dataframe
  (define (combine-names-ordered . alists)
    (define (loop all-names results)
      (cond [(null? all-names)
             (reverse results)]
            [(member (car all-names) results)
             (loop (cdr all-names) results)]
            [else
             (loop (cdr all-names) (cons (car all-names) results))]))
    (loop (apply get-all-names alists) '()))

  
  ;; assertions ------------------------------------------------------------------------
  
  (define (check-list ls ls-name who)
    (unless (list? ls)
      (assertion-violation who (string-append ls-name " is not a list")))
    (when (null? ls)
      (assertion-violation who (string-append ls-name " is an empty list"))))

  (define (check-integer-positive x x-name who)
    (unless (and (> x 0) (integer? x))
      (assertion-violation who (string-append x-name " is not a positive integer"))))
  
  (define (check-integer-gte-zero x x-name who)
    (unless (and (>= x 0) (integer? x))
      (assertion-violation who (string-append x-name " is not an integer >= 0"))))

  (define (check-index n n-max who)
    (when (> n n-max)
      (assertion-violation who (string-append "index " (number->string n) " is out of range"))))

  (define (check-names-unique names who)
    (unless (= (length names) (length (remove-duplicates names)))
      (assertion-violation who "names are not unique")))

  (define (check-names-symbol names who)
    (unless (for-all (lambda (name) (symbol? name)) names)
      (assertion-violation who "names are not symbols")))

  (define (check-names names who)
    (check-names-symbol names who)
    (check-names-unique names who))

  (define (check-names-duplicate old-names new-names who)
    (unless (for-all (lambda (new-name) (not (member new-name old-names))) new-names)
      (assertion-violation who "new names duplicate existing names")))

  (define (check-new-names old-names new-names who)
    (check-names new-names who)
    (check-names-duplicate old-names new-names who))
  
  (define (check-name-pairs current-names name-pairs who)
    ;; not very thorough checking of ways a name-pair could be malformed
    (unless (for-all pair? name-pairs)
      (assertion-violation who "names not of form '(old-name new-name) ..."))
    (let ([new-names (map cadr name-pairs)])
      (check-new-names current-names new-names who)))

  ;; lots of checking that will be performed every time a dataframe is created
  ;; this currently allows for creating a dataframe with no rows
  ;; even though none of the dataframe procedures will accept a df with zero rows
  (define (check-alist alist who)
    (when (null? alist)
      (assertion-violation who "alist is empty"))
    (unless (list? alist)
      (assertion-violation who "alist is not a list"))
    (unless (list? (car alist))
      (assertion-violation who "(car alist) is not a list"))
    (let ([names (map car alist)])
      (check-names-symbol names who)
      (check-names-unique names who))
    (unless (for-all (lambda (col) (list? (cdr col))) alist)
      (assertion-violation who "values are not a list"))
    (let ([col-lengths (map length alist)])
      ;; if only one column don't need to check equal length
      (unless (or (= (length col-lengths) 1)
                  (apply = (map length alist)))
        (assertion-violation who "columns not all same length"))))
  
  (define (make-list n x)
    (let loop ((n n) (r '()))
      (if (= n 0)
          r
          (loop (- n 1) (cons x r)))))

  (define (sub1 n) (- n 1))
  (define (add1 n) (+ n 1))

  ;; simplified SRFI 1 iota (regular version will work)
  (define (iota count)
    (define start 0)
    (define step 1)
    (let loop ((n 0) (r '()))
      (if (= n count)
	  (reverse r)
	  (loop (+ 1 n)
	        (cons (+ start (* n step)) r)))))

  (define (enumerate lst)
    (iota (length lst)))

  ;; from SRFI-1 `take`
  (define (list-head lst k)
    (let recur ((lst lst) (k k))
      (if (zero? k) '()
	  (cons (car lst)
	        (recur (cdr lst) (- k 1))))))
  )




