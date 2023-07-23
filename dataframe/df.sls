(library (dataframe df)
  (export
   ->
   ->>
   )

  (import (rnrs)
          (dataframe record-types)
          (only (dataframe helpers)
                not-in))






  



  
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

