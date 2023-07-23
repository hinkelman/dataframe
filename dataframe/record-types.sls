(library (dataframe record-types)
  (export
   ->
   ->>
   check-dataframe
   check-all-dataframes
   check-names-exist
   check-df-names
   dataframe-contains?
   dataframe?
   dataframe-equal?
   dataframe-slist
   dataframe-names
   dataframe-dim
   make-df*
   make-dataframe
   make-series*
   make-series
   make-slist
   series?
   series-equal?
   series-name
   series-lst
   series-length)

  (import (rnrs)
          (dataframe types)
          (only (dataframe assertions)
                check-names))
                
  ;; series -------------------------------------------------------------------

  (define-record-type series
    ;; series-src is not exported
    (fields name src lst type length)
    (protocol
     (lambda (new)
       (lambda (name src)
         (let* ([type (guess-type src 1000)]
                [lst (convert-type src type)])
           (new name src lst type (length lst)))))))

  (define-syntax make-series*
    (syntax-rules ()
      [(_ (name vals ...))
       (make-series (quote name) (list vals ...))]))

  (define (series-equal? . series)
    (let* ([first-name (series-name (car series))]
           [first-lst (series-lst (car series))])
      (for-all (lambda (x)
                 (and (equal? (series-name x) first-name)
                      (equal? (series-lst x) first-lst)))
               series)))
  
  ;; dataframe ----------------------------------------------------------------

  (define-record-type dataframe
    (fields slist names dim)
    (protocol
     (lambda (new)
       (lambda (slist)
         (let* ([names (map series-name slist)]
                [rows (series-length (car slist))]
                [cols (length names)])
           (new slist names (cons rows cols)))))))

  (define-syntax make-df*
    (syntax-rules ()
      [(_ (name vals ...) ...)
       (let ([names (list (quote name) ...)]
             [ls-vals (list (list vals ...) ...)])
         (make-dataframe (make-slist names ls-vals)))]))
  
  (define (make-slist names ls-vals)
    (map (lambda (name vals)
           (make-series name vals))
         names ls-vals))

  (define (dataframe-equal? . dfs)
    (let ([first-names (dataframe-names (car dfs))]
          [first-dim (dataframe-dim (car dfs))]
          [first-slist (dataframe-slist (car dfs))])
      (for-all
       (lambda (df)
         (and (equal? (dataframe-names df) first-names)
              (equal? (dataframe-dim df) first-dim)
              (for-all
               (lambda (series1 series2)
                 (series-equal? series1 series2))
               (dataframe-slist df) first-slist)))
       dfs)))

  ;; check dataframes -----------------------------------------------------------
  
  (define (check-dataframe df who)
    (unless (dataframe? df)
      (assertion-violation who "df is not a dataframe"))
    (unless (> (car (dataframe-dim df)) 0)
      (assertion-violation who "df has zero rows")))

  (define (check-all-dataframes dfs who)
    (map (lambda (df) (check-dataframe df who)) dfs))

  ;; check dataframe attributes -------------------------------------------------------------
  
  (define (dataframe-contains? df . names)
    (check-dataframe df "(dataframe-contains? df names)")
    (let ([df-names (dataframe-names df)])
      (for-all (lambda (name) (member name df-names)) names)))

  (define (check-names-exist df who . names)
    (unless (apply dataframe-contains? df names)
      (assertion-violation who "name(s) not in df")))

  (define (check-df-names df who . names)
    (check-dataframe df who)
    (check-names names who)
    (apply check-names-exist df who names))

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
  

    ;; lots of checking that will be performed every time a dataframe is created
  ;; this currently allows for creating a dataframe with no rows
  ;; even though none of the dataframe procedures will accept a df with zero rows
  ;; (define (check-alist alist who)
  ;;   (when (null? alist)
  ;;     (assertion-violation who "alist is empty"))
  ;;   (unless (list? alist)
  ;;     (assertion-violation who "alist is not a list"))
  ;;   (unless (list? (car alist))
  ;;     (assertion-violation who "(car alist) is not a list"))
  ;;   (let ([names (map car alist)])
  ;;     (check-names-symbol names who)
  ;;     (check-names-unique names who))
  ;;   (unless (for-all (lambda (col) (list? (cdr col))) alist)
  ;;     (assertion-violation who "values are not a list"))
  ;;   (let ([col-lengths (map length alist)])
  ;;     ;; if only one column don't need to check equal length
  ;;     (unless (or (= (length col-lengths) 1)
  ;;                 (apply = (map length alist)))
  ;;       (assertion-violation who "columns not all same length"))))
  
  )

