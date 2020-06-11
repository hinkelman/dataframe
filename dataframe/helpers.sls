(library (dataframe helpers)
  (export
   add-names-ls-vals
   alist-modify
   alist-select
   alist-values
   alist-values-map
   combine-names-ordered
   filter-ls-vals
   filter-vals
   get-all-names
   get-all-unique-names
   check-index
   check-integer-gte-zero
   check-integer-positive
   check-list
   check-names-unique
   check-names-symbol
   check-names
   check-names-duplicate
   check-new-names
   check-name-pairs
   check-alist
   partition-ls-vals
   remove-duplicates
   transpose
   unique-rows)

  (import (chezscheme))

  ;; https://stackoverflow.com/questions/8382296/scheme-remove-duplicated-numbers-from-list
  (define (remove-duplicates ls)
    (cond [(null? ls)
           '()]
          [(member (car ls) (cdr ls))
           (remove-duplicates (cdr ls))]
          [else
           (cons (car ls) (remove-duplicates (cdr ls)))]))
  
  (define (transpose ls)
    (apply map list ls))

  ;; ls-vals ------------------------------------------------------------------------
  
  ;; add names to list of vals, ls-vals, to create association list
  (define (add-names-ls-vals names ls-vals)
    (if (null? ls-vals)
        (map (lambda (name) (cons name '())) names)
        (map (lambda (name vals) (cons name vals)) names ls-vals)))

  ;; takes ls-vals and returns unique rows in those ls-vals
  ;; returns row-based or column-based result
  (define (unique-rows ls-vals row-based)
    (let ([ls-rows (remove-duplicates (transpose ls-vals))])
      (if row-based ls-rows (transpose ls-rows))))

  ;; two passes through ls-vals
  ;; recursive solution might be more efficient
  ;; currently just a shorthand way of writing two filters
  (define (partition-ls-vals bools ls-vals)
    (let ([keep (filter-ls-vals bools ls-vals)]
          [drop (filter-ls-vals (map not bools) ls-vals)])
      (values keep drop)))

  (define (filter-ls-vals bools ls-vals)
    (map (lambda (vals) (filter-vals bools vals)) ls-vals))

  ;; filter vals by list of booleans of same length as vals
  (define (filter-vals bools vals)
    (let ([bools-vals (map cons bools vals)])
      (map cdr (filter (lambda (x) (car x)) bools-vals))))

  ;; alists ------------------------------------------------------------------------

  (define (alist-select alist names)
    (map (lambda (name) (assoc name alist)) names))

  (define (alist-values alist name)
    (cdr (assoc name alist)))

  (define (alist-values-map alist names)
    (map (lambda (name) (alist-values alist name)) names))

  ;; update alist and or add column to end of alist
  ;; based on whether name is already present in alist
  (define (alist-modify alist name vals)
    (let ([col (cons name vals)]
          [all-names (map car alist)])
      (if (member name all-names)
          (map (lambda (x)
                 (if (symbol=? x name) col (assoc x alist)))
               all-names)
          ;; to get correct structure with append,
          ;; need to make col an alist (by wrapping in a list) 
          (append alist (list col))))) 

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
      (assertion-violation who "names not of form '((old-name1 new-name1) (old-name2 new-name2))"))
    (let ([new-names (map cadr name-pairs)])
      (check-new-names current-names new-names who)))

  (define (same-length? len ls)
    (= len (length ls)))
  
  ;; lots of checking that will be performed every time a dataframe is created
  (define (check-alist alist who)
    (when (null? alist)
      (assertion-violation who "alist is empty"))
    (unless (list? alist)
      (assertion-violation who "alist is not a list"))
    (unless (list? (car alist))
      (assertion-violation who "(car alist) is not a list"))
    (when (list? (cadar alist))
      (assertion-violation who "(cadar alist) is a list"))
    (let ([names (map car alist)])
      (check-names-symbol names who)
      (check-names-unique names who))
    (unless (for-all (lambda (col) (list? (cdr col))) alist)
      (assertion-violation who "values are not a list"))
    (unless (apply = (map length alist))
      (assertion-violation who "columns not all same length")))

  
  )




