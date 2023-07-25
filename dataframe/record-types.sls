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
   series-length
   series-type
   slist-repeat-rows)

  (import (rnrs)
          (dataframe types)
          (only (dataframe helpers)
                rep)
          (only (dataframe assertions)
                check-names
                check-names-symbol
                check-names-unique))
                
  ;; series -------------------------------------------------------------------

  (define-record-type series
    ;; series-src is not exported
    (fields name src lst type length)
    (protocol
     (lambda (new)
       (lambda (name src)
         (check-series name src "(make-series name src)")
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

  (define (check-series name src who)
    (when (null? src)
      (assertion-violation who "src is empty"))
    (unless (list? src)
      (assertion-violation who "src is not a list"))
    (check-names-symbol (list name) who))

  ;; dataframe ----------------------------------------------------------------

  (define-record-type dataframe
    (fields slist names dim)
    (protocol
     (lambda (new)
       (lambda (slist)
         (check-slist slist "(make-dataframe slist)")
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

  ;; expand a slist by repeating rows n times (or each)
  (define (slist-repeat-rows slist n type)
    (map (lambda (series)
           (make-series (series-name series)
                        (rep (series-lst series) n type)))
         slist))

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

  ;; check slist ------------------------------------------------------------------------------

  ;; checking that will be performed every time a dataframe is created
  ;; this currently allows for creating a dataframe with no rows
  ;; even though none of the dataframe procedures will accept a df with zero rows
  (define (check-slist slist who)
    (when (null? slist)
      (assertion-violation who "slist is empty"))
    (unless (for-all series? slist)
      (assertion-violation who "all elements of slist are not series"))
    (check-names-unique (map series-name slist) who)
    (let ([col-lengths (map series-length slist)])
      ;; if only one column don't need to check equal length
      (unless (or (= (length col-lengths) 1)
                  (apply = (map series-length slist)))
        (assertion-violation who "series are not all same length"))))
  
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

