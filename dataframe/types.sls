(library (dataframe types)
  (export
   count
   count-elements
   convert-type
   get-type
   guess-type)

  (import (rnrs)
          (only (dataframe helpers)
                list-head
                na?
                remove-duplicates))

  (define (convert-type obj type)
    (cond [(or (na? type) (symbol=? type 'other))
           (if (or (na? obj) (na-string? obj)) 'na obj)]
          ;; string->number is only attempted automatic conversion 
          [(symbol=? type 'num)
           (cond [(and (string? obj) (string->number obj))
                  (string->number obj)]
                 [(not (number? obj)) 'na]
                 [else obj])]
          [(symbol=? type 'str)
           (let ([->string (get->string obj)])
             (cond [(or (na? obj) (na-string? obj)) 'na]
                   [(string? obj) obj]
                   [(->string obj) (->string obj)]
                   [else 'na]))]
          [else (obj->na obj)]))


  (define (guess-type lst n-max)
    (if (null? lst)
        'na
        (let* ([n (length lst)]
               [actual-max (if (< n n-max) n n-max)]
               [types (map get-type (list-head lst actual-max))]
               [first (car types)]
               [type-count (count-elements types)]
               [type-count-n (length type-count)])
          (cond [(member 'other types)
                 'other]
                [(= type-count-n 1)
                 first]
                ;; na's don't count against same type
                [(and (= type-count-n 2)
                      (na? (caar type-count)))
                 (caadr type-count)]
                [(and (= type-count-n 2)
                      (na? (caadr type-count)))
                 (caar type-count)]
                [else
                 'str]))))

  (define (count-elements lst)
    (map (lambda (x) (cons x (count x lst)))
         (remove-duplicates lst)))

  (define (count obj lst)
    (length (filter (lambda (x) (equal? obj x)) lst)))
  
  (define (get->string obj)
    ;; proc that converts an object to string; if not, return false
    (let loop ([procs (list bool->string num->string
                            sym->string char->string)])
      (cond [(null? procs) ;; no matching type
             (lambda (x) #f)]
            [((car procs) obj)
             (car procs)]
            [else
             (loop (cdr procs))])))

  (define (get-type obj)
    (cond [(or (na? obj) (na-string? obj))
           'na]
          ;; string->number is only attempted automatic conversion 
          [(and (string? obj)
                (or (na-string? obj)
                    (string->number obj)))
           'num]
          ;; loop through other predicates
          [else
           (get-type-loop obj)]))

  (define (get-type-loop obj)
    (let loop ([preds (list boolean? number? symbol? char? string?)]
               [types '(bool num sym chr str)])
      (cond [(null? preds) ;; no matching type 
             'other]
            [((car preds) obj)
             (car types)]
            [else
             (loop (cdr preds) (cdr types))])))

  (define (na-string? obj)
    (and (string? obj)
         (or (string=? obj "")
             (string=? obj " ")
             (string=? obj "NA")
             (string=? obj "na"))))

  (define (obj->na obj)
    ;; sets value to 'na if not one of three types
    (let loop ([preds (list boolean? symbol? char?)]
               [types '(boolean symbol char)])
      (cond [(null? preds) 'na]
            [((car preds) obj) obj]
            [else (loop (cdr preds) (cdr types))])))

  (define (bool->string obj)
    (if (boolean? obj) (if obj "#t" "#f") #f))

  (define (num->string obj)
    (if (number? obj) (number->string obj) #f))

  (define (sym->string obj)
    (if (symbol? obj) (symbol->string obj) #f))

  (define (char->string obj)
    (if (char? obj) (string obj) #f))
  
  )

