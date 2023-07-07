(library (dataframe df)
  (export
   ->
   ->>
   $
   check-all-dataframes
   check-dataframe
   check-df-names
   check-names-exist
   dataframe?
   dataframe-alist
   dataframe-contains?
   dataframe-crossing
   dataframe-dim
   dataframe-drop
   dataframe-equal?
   dataframe-head
   dataframe-names
   dataframe-read
   dataframe-ref
   dataframe-rename
   dataframe-rename-all
   dataframe-select
   dataframe-tail
   dataframe-unique
   dataframe-values
   dataframe-values-map
   dataframe-values-unique
   dataframe-write
   make-dataframe)

  (import (rnrs)
          (dataframe helpers))

  ;; naming conventions ----------------------------------------------------------------------

  ;; dataframe: a record-type comprised of an association list that meets specified criteria
  ;; alist: list at the core of a dataframe; form is '((a 1 2 3) (b "this" "that" "other"))
  ;; alists: list of alists
  ;; column: one of the lists in the alist, includes name as first element
  ;; vals: one of the lists in the alist, but with the name excluded
  ;; ls-vals: vals from alist packaged into a list, e.g., '((1 2 3) ("this" "that" "other"))
  ;; name: column name
  ;; names: list of column names
  ;; group-names: list of column names used for grouping
  ;; df: single dataframe
  ;; dfs (or df-list): list of dataframes
  ;; ls or lst: generic list
  ;; x: generic object
  ;; procedure:  procedure
  ;; procedures: list of procedures
  ;; bools: list of boolean values '(#t, #f)
  ;; row-based: describes orientation of list of lists; row-based example: '((a b) (1 "this") (2 "that") (3 "other))
  ;; rowtable: name used to describe a row-based list of lists where the first row represents column names
  ;; rt: single rowtable
  ;; ls-rows: generic row-based list of lists
  
  ;; dataframe record type ---------------------------------------------------------------------

  (define-record-type dataframe (fields alist names dim)
                      (protocol
                       (lambda (new)
                         (lambda (alist)
                           (let ([who "(make-dataframe alist)"])
                             (check-alist alist who))
                           (new alist
                                (map car alist)
                                (cons (length (cdar alist)) (length alist)))))))

  ;; crossing/cartesian-product ----------------------------------------------------------------

  (define (dataframe-crossing . obj)
    ;; doesn't currently have very informative error messages
    (let* ([names (flatten (map (lambda (x) (if (dataframe? x)
                                                (dataframe-names x)
                                                (car x)))
                                obj))]
           ;; transpose the col-oriented alist to keep rows as a unit in the cartesian product
           [lst (map (lambda (x) (if (dataframe? x)
                                     (transpose (map cdr (dataframe-alist x)))
                                     (cdr x)))
                     obj)]
           [ls-vals (transpose (map flatten (apply cartesian-product lst)))])
      (make-dataframe (add-names-ls-vals names ls-vals))))

  ;; check dataframes --------------------------------------------------------------------------
  
  (define (check-dataframe df who)
    (unless (dataframe? df)
      (assertion-violation who "df is not a dataframe"))
    (unless (> (car (dataframe-dim df)) 0)
      (assertion-violation who "df has zero rows")))

  (define (check-all-dataframes dfs who)
    (map (lambda (df) (check-dataframe df who)) dfs))

  (define (dataframe-equal? . dfs)
    (check-all-dataframes dfs "(dataframe-equal? dfs)")
    (let* ([alists (map dataframe-alist dfs)]
           [first-alist (car alists)])
      (for-all (lambda (alist)
                 (equal? alist first-alist))
               alists)))

  ;; check dataframe attributes -------------------------------------------------------------
  
  (define (dataframe-contains? df . names)
    (check-dataframe df "(dataframe-contains? df names)")
    (let ([df-names (dataframe-names df)])
      (if (for-all (lambda (name) (member name df-names)) names) #t #f)))

  (define (check-names-exist df who . names)
    (unless (apply dataframe-contains? df names)
      (assertion-violation who "name(s) not in df")))

  (define (check-df-names df who . names)
    (check-dataframe df who)
    (check-names names who)
    (apply check-names-exist df who names))
  
  ;; head/tail -----------------------------------------------------------------------------------

  (define (dataframe-head df n)
    (let ([who  "(dataframe-head df n)"])
      (check-dataframe df who)
      (check-integer-positive n "n" who)
      (check-index n (car (dataframe-dim df)) who)
      (make-dataframe (alist-head-tail (dataframe-alist df) n list-head))))

  ;; dataframe-tail is based on list-tail, which does not work the same as tail in R
  (define (dataframe-tail df n)
    (let ([who  "(dataframe-tail df n)"])
      (check-dataframe df who)
      (check-integer-gte-zero n "n" who)
      (check-index (add1 n) (car (dataframe-dim df)) who)
      (make-dataframe (alist-head-tail (dataframe-alist df) n list-tail))))

  (define (alist-head-tail alist n proc)
    (map (lambda (col) (cons (car col) (proc (cdr col) n))) alist))

  ;; unique ------------------------------------------------------------------------

  (define (dataframe-unique df)
    (check-dataframe df "(dataframe-unique df)")
    (make-dataframe (alist-unique (dataframe-alist df))))
  
  (define (alist-unique alist)
    (let ([names (map car alist)]
          [ls-vals (map cdr alist)])
      (add-names-ls-vals
       names
       (transpose (remove-duplicates (transpose ls-vals))))))
  
  ;; extract values -----------------------------------------------------------------

  ;; returns simple list
  (define (dataframe-values df name)
    (let ([who "(dataframe-values df name)"])
      (check-dataframe df who)
      (check-names-exist df who name))
    (alist-values (dataframe-alist df) name))

  (define ($ df name)
    (dataframe-values df name))

  (define (dataframe-values-unique df name)
    (let ([who "(dataframe-values-unique df name)"])
      (check-dataframe df who)
      (check-names-exist df who name))
    (cdar (alist-unique (alist-select (dataframe-alist df) (list name)))))

  (define (dataframe-values-map df names)
    (alist-values-map (dataframe-alist df) names))

  ;; dataframe-ref -------------------------------------------------------------------

  (define dataframe-ref
    (case-lambda
      [(df indices) (df-ref-helper df indices (dataframe-names df))]
      [(df indices . names)(df-ref-helper df indices names)]))

  (define (df-ref-helper df indices names)
    (let ([who "(dataframe-ref df indices)"]
          [n-max (car (dataframe-dim df))])
      (apply check-df-names df who names)
      (check-list indices "indices" who)
      (map (lambda (n)
             (check-integer-gte-zero n "index" who)
             (check-index n n-max who))
           indices))
    (make-dataframe (alist-ref (dataframe-alist df) indices names)))
  
  ;; read/write ------------------------------------------------------------------------------
  
  (define (dataframe-write df path overwrite)
    (when (and (file-exists? path) (not overwrite))
      (assertion-violation path "file already exists"))
    (when (file-exists? path)
      (delete-file path))
    (with-output-to-file path
      (lambda () (write (dataframe-alist df)))))

  (define (dataframe-read path)
    (make-dataframe (with-input-from-file path read)))

  ;; select/drop columns ------------------------------------------------------------------------

  (define (dataframe-select df . names)
    (apply check-df-names df "(dataframe-select df names)" names)
    (make-dataframe (alist-select (dataframe-alist df) names)))

  (define (dataframe-drop df . names)
    (apply check-df-names df "(dataframe-drop df names)" names)
    (make-dataframe (alist-drop (dataframe-alist df) names)))


  ;; rename columns ---------------------------------------------------------------------------------

  (define (dataframe-rename df . name-pairs)
    (let ([who "(dataframe df '(old-name new-name) ...)"])
      (check-dataframe df who)
      (check-name-pairs (dataframe-names df) name-pairs who))
    (make-dataframe (map (lambda (column)
                           (let* ([name (car column)]
                                  [ls-vals (cdr column)]
                                  [name-match (assoc name name-pairs)])
                             (if name-match
                                 (cons (cadr name-match) ls-vals)
                                 column)))
                         (dataframe-alist df))))

  (define (dataframe-rename-all df names)
    (let ([who "(dataframe-rename-all df names)"])
      (check-dataframe df who)
      (check-names names who)
      (let ([names-length (length names)]
            [num-cols (cdr (dataframe-dim df))])
        (unless (= names-length num-cols)
          (assertion-violation who (string-append
                                            "names length must be "
                                            (number->string num-cols)
                                            ", not "
                                            (number->string names-length)))))
      (let* ([alist (dataframe-alist df)]
             [ls-vals (map cdr alist)])
        (make-dataframe (add-names-ls-vals names ls-vals)))))

  ;; thread-first and thread-last -------------------------------------------------------------

  ;; https://github.com/ar-nelson/srfi-197/commit/c9b326932d7352a007e25051cb204ad7e9945a45
  (define-syntax ->
    (syntax-rules ()
      [(-> x) x]
      [(-> x (fn . args) . rest)
       (-> (fn x . args) . rest)]
      [(-> x fn . rest)
       (-> (fn x) . rest)]))

  (define-syntax ->>
    (syntax-rules ()
      [(->> x) x]
      [(->> x (fn args ...) . rest)
       (->> (fn args ... x) . rest)]
      [(->> x fn . rest)
       (->> (fn x) . rest)]))
  
  )

