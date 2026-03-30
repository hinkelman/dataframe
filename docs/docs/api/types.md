# Types

## `(get-type obj)`

**returns:** type of `obj` as a symbol: one of `bool`, `chr`, `str`, `sym`, `num`, or `other`. Strings that are valid numbers are assumed to be `num`.

```scheme
> (get-type "3")
num

> (get-type '(1 2 3))
other
```

---

## `(guess-type lst n-max)`

**returns:** type of the elements in `lst` as a symbol; evaluates up to `n-max` elements before guessing. Strings that are valid numbers are assumed to be `num`.

```scheme
> (guess-type '(1 2 3) 3)
num

> (guess-type '(1 "2" 3) 3)
num

> (guess-type '(a b c) 3)
sym

> (guess-type '(a b "c") 3)
str

> (guess-type '(a b "c") 2)
sym
```

---

## `(convert-type obj type)`

**returns:** `obj` converted to `type`; elements that cannot be converted are replaced with `'na`.

!!! note
    Converting a symbol or character to `'num` returns `'na`. Converting a string to `'sym` also returns `'na`, to avoid surprising results like `(string->symbol "10")` → `\x31;0`.

```scheme
> (convert-type "c" 'sym)
na

> (convert-type 'b 'str)
"b"

> (map (lambda (x) (convert-type x 'other)) '(a b "c"))
(a b "c")

> (convert-type "3" 'num)
3

> (map (lambda (x) (convert-type x 'num)) '(1 2 3 na "" " " "NA" "na"))
(1 2 3 na na na na na)

> (map (lambda (x) (convert-type x 'str)) '(a "b" c na "" " " "NA" "na"))
("a" "b" "c" na na na na na)
```
