(library (dataframe types)
  (export convert-type guess-type)

  (import (rnrs)
          (dataframe statistics))

  (define (convert-type lst type)
    (cond [(symbol=? type 'other) lst]
          ;; string->number is only attempted automatic conversion 
          [(symbol=? type 'number)
           (map (lambda (x)
                  (cond [(null? x) '()]
                        [(and (string? x) (string->number x))
                         (string->number x)]
                        [(not (number? x)) '()]
                        [else x]))
                lst)]
          [(symbol=? type 'string)
           (map (lambda (x)
                  (let ([->string (get->string x)])
                    (cond [(string? x) x]
                          [(->string x) (->string x)]
                          [else '()])))
                lst)]
          [else (map obj->null lst)]))

  (define (guess-type lst n-max)
    ;; need to add check for empty vector
    (let* ([n (length lst)]
           [actual-max (if (< n n-max) n n-max)]
           [types (map get-type (list-head lst actual-max))]
           [first (car types)]
           [type-count (count-elements types)]
           [type-count-n (length type-count)])
      (cond [(or (member 'other types)
                 (and (= type-count-n 1) (null? first)))
             'other]
            [(= type-count-n 1)
             first]
            ;; nulls don't count against same type
            [(and (= type-count-n 2)
                  (null? (caar type-count)))
             (caadr type-count)]
            [(and (= type-count-n 2)
                  (null? (caadr type-count)))
             (caar type-count)]
            [else
             'string])))
  
  (define (get->string obj)
    ;; proc that converts an object to string; if not, return false
    (let loop ([procs (list bool->string date->string num->string
                            sym->string char->string)])
      (cond [(null? procs) ;; no matching type
             (lambda (x) #f)]
            [((car procs) obj)
             (car procs)]
            [else
             (loop (cdr procs))])))

  (define (get-type object)
    (let loop ([preds (list boolean? date? number? symbol?
                            char? string? null?)]
               [types '(boolean date number symbol
                                char string ())])
      (cond [(null? preds) ;; no matching type 
             'other]
            ;; string->number is only attempted automatic conversion 
            [(and (string? object) (string->number object))
             'number]
            [((car preds) object)
             (car types)]
            [else
             (loop (cdr preds) (cdr types))])))

  (define (obj->null object)
    ;; sets value to '() if not one of four types
    (let loop ([preds (list boolean? date? symbol? char?)]
               [types '(boolean date symbol char)])
      (cond [(null? preds) '()]
            [((car preds) object) object]
            [else (loop (cdr preds) (cdr types))])))

  (define (bool->string obj)
    (if (boolean? obj) (if obj "#t" "#f") #f))

  (define (date->string obj)
    (if (date? obj) (date-and-time obj) #f))

  (define (num->string obj)
    (if (number? obj) (number->string obj) #f))

  (define (sym->string obj)
    (if (symbol? obj) (symbol->string obj) #f))

  (define (char->string obj)
    (if (char? obj) (string obj) #f))
  
  )

