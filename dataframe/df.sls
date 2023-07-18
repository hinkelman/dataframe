(library (dataframe df)
  (export
   ->
   ->>
   )

  (import (rnrs)
          (dataframe record-types)
          (only (dataframe helpers)
                not-in))



  ;; check dataframes --------------------------------------------------------------------------
  
  ; (define (check-dataframe df who)
  ;   (unless (dataframe? df)
  ;     (assertion-violation who "df is not a dataframe"))
  ;   (unless (> (car (dataframe-dim df)) 0)
  ;     (assertion-violation who "df has zero rows")))

  ; (define (check-all-dataframes dfs who)
  ;   (map (lambda (df) (check-dataframe df who)) dfs))

  ; (define (dataframe-equal? . dfs)
  ;   (check-all-dataframes dfs "(dataframe-equal? dfs)")
  ;   (let* ([alists (map dataframe-alist dfs)]
  ;          [first-alist (car alists)])
  ;     (for-all (lambda (alist)
  ;                (equal? alist first-alist))
  ;              alists)))

  ;; check dataframe attributes -------------------------------------------------------------
  
  ; (define (dataframe-contains? df . names)
  ;   (check-dataframe df "(dataframe-contains? df names)")
  ;   (let ([df-names (dataframe-names df)])
  ;     (if (for-all (lambda (name) (member name df-names)) names) #t #f)))

  ; (define (check-names-exist df who . names)
  ;   (unless (apply dataframe-contains? df names)
  ;     (assertion-violation who "name(s) not in df")))

  ; (define (check-df-names df who . names)
  ;   (check-dataframe df who)
  ;   (check-names names who)
  ;   (apply check-names-exist df who names))
  



  
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

