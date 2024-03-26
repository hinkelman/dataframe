(library (dataframe types)
  (export
   count
   count-elements
   convert-type
   guess-type)

  (import (rnrs)
          (only (dataframe helpers)
                list-head
                na?
                remove-duplicates))

  (define (convert-type lst type)
    (cond [(or (na? type) (symbol=? type 'other))
           (map (lambda (x)
                  (if (or (na? x) (na-string? x)) 'na x)) lst)]
          ;; string->number is only attempted automatic conversion 
          [(symbol=? type 'num)
           (map (lambda (x)
                  (cond [(and (string? x) (string->number x))
                         (string->number x)]
                        [(not (number? x)) 'na]
                        [else x]))
                lst)]
          [(symbol=? type 'str)
           (map (lambda (x)
                  (let ([->string (get->string x)])
                    (cond [(or (na? x) (na-string? x)) 'na]
                          [(string? x) x]
                          [(->string x) (->string x)]
                          [else 'na])))
                lst)]
          [else (map obj->na lst)]))

  (define (guess-type lst n-max)
    ;; need to add check for empty vector
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
             'str])))

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

  (define (get-type object)
    (cond [(or (na? object) (na-string? object))
           'na]
          ;; string->number is only attempted automatic conversion 
          [(and (string? object)
                (or (na-string? object)
                    (string->number object)))
           'num]
          ;; loop through other predicates
          [else
           (get-type-loop object)]))

  (define (get-type-loop object)
    (let loop ([preds (list boolean? number? symbol? char? string?)]
               [types '(bool num sym chr str)])
      (cond [(null? preds) ;; no matching type 
             'other]
            [((car preds) object)
             (car types)]
            [else
             (loop (cdr preds) (cdr types))])))

  (define (na-string? object)
    (and (string? object)
         (or (string=? object "")
             (string=? object " ")
             (string=? object "NA")
             (string=? object "na"))))

  (define (obj->na object)
    ;; sets value to 'na if not one of three types
    (let loop ([preds (list boolean? symbol? char?)]
               [types '(boolean symbol char)])
      (cond [(null? preds) 'na]
            [((car preds) object) object]
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

