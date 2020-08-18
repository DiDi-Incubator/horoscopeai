grammar Flow;

@lexer::members {
    int nesting = 0;
}

wholeFlow
    : flow EOF
    ;

wholeExpression
    : expression EOF
    ;

flow
    : '#' URI NL (flowConfig NL | NL)* declaration '***' NL block '***' NL*
    ;

flowConfig
    : '*' SNAKE '=' STRING
    ;

declaration
    : (compositor NL | NL)*
    ;

compositor
    : '*' UCAMEL NL CODE
    ;

block
    :(statement NL | NL)*
    ;

statement
    : variable ('<-' | isLazy='<~') evaluate # Assign
    | (variable ('<-' | isLazy='<~'))? UCAMEL LPAREN compositeArgumentList RPAREN # Composite
    | (variable ('<-' | isLazy='<~'))? LBRACK UCAMEL LPAREN ('_' | compositeArgumentList) RPAREN '<-' expression RBRACK # BatchComposite
    | '<>' '{' (choice NL| NL)+ '}' # Branch
    | '<' SNAKE '>' '+' STRING? '=>' URI flowArgumentList ('#' trace=evaluate)? # Schedule
    | '<' SNAKE '>' '=>' URI flowArgumentList # Include
    ;

compositeArgumentList
    : compositeArgument (',' compositeArgument)*
    |
    ;

compositeArgument
    : name ('=' expression)?
    ;

flowArgumentList
    :'?' namedExpression ('&' namedExpression)*
    |
    ;

gotoArgumentList
    : LPAREN namedExpression (',' namedExpression)* RPAREN
    |
    ;

choice
    : '?' namedExpression ('|' namedExpression | NL)+ '=>' '{' block '}'
    ;

namedExpression
    : name ('=' evaluate)?
    ;

evaluate
    : expr=expression ('!!' failover=expression)?
    ;

expression
    : value
    | predicate
    ;

value
    : LPAREN value RPAREN # SubValue
    | ('_' | '#') # Placeholder
    | LCAMEL # Member
    | (scope=SNAKE '->')? variable # Reference
    | primitive # Literal
    | function LPAREN (expression (',' expression)*)? RPAREN # Call
    | from=value '.' function LPAREN (expression (',' expression)*)? RPAREN # Apply
    | obj # NewObject
    | array # NewArray
    | from=value LBRACK index=value RBRACK # At
    | from=value '.' property # Visit
    | from=value '.' '*' # VisitAll
    | from=value '..' property # Search
    | from=value '..' '*' # SearchAll
    | from=value LBRACK begin=expression? ':' end=expression? RBRACK # Slice
    | from=value LBRACK indices RBRACK # Select
    | from=value LBRACK '+' expression RBRACK # Expand
    | from=value LBRACK '-' RBRACK # Fold
    | from=value LBRACK '*' RBRACK # SelectAll
    | from=value LBRACK '/' LPAREN expression RPAREN RBRACK # Project
    | from=value LBRACK '?' LPAREN predicate RPAREN RBRACK # Query
    | from=value '\'' # Transpose
    | LBRACK expression '<-' from=value RBRACK # Transform
    | '-' value # Minus
    | value op=('*' | '/' | '%') value # MulDiv
    | value op=('+' | '-') value # AddSub
    ;

predicate
    : LPAREN predicate RPAREN # SubPredicate
    | 'not' predicate # Not
    | value 'in' value # In
    | value 'not' 'in' value # NotIn
    | value op=('<' | '<=' | '>' | '>=') value # Compare
    | value op=('==' | '!=') value # EqualOrNot
    | predicate 'and' predicate # And
    | predicate 'or' predicate # Or
    | value # ToBoolean
    ;

obj
    : ('{') pair (',' pair)* ('}')
    ;

pair
    : NL* STRING NL* ':' NL* expression NL*
    ;

array
    : LBRACK (expression (',' expression)*)? RBRACK
    ;

indices
    : value? (',' value)+
    ;

variable
    : ('@' | '$')? name
    | '@'
    | '$'
    ;

name
    : SNAKE
    ;

property
    : SNAKE
    | LCAMEL
    | UCAMEL
    | keyword
    ;

function
    : SNAKE
    | LCAMEL
    ;

keyword
    : NULL
    | BOOLEAN
    | 'and'
    | 'or'
    | 'not'
    | 'in'
    ;

primitive
    : STRING
    | NUMBER
    | BOOLEAN
    | NULL
    ;


STRING
    : '"' (ESC | SAFECODEPOINT)* '"'
    ;

NUMBER
    : '-'? INT ('.' [0-9] +)? EXP?
    ;

BOOLEAN
    : 'true'
    | 'false'
    ;

NULL
    : 'null'
    ;

SNAKE
    : [_a-z] [_a-z0-9]*
    ;

UCAMEL
    : [A-Z] [a-zA-Z0-9]*
    ;

LCAMEL
    : [a-z] [a-zA-Z0-9]*
    ;

URI
    : ('/' ~ [ ?#"()\\\u0000-\u001F]+)+
    ;

CODE
    : '```' .*? '```'
    ;


fragment ESC
    : '\\' (["\\/bfnrt] | UNICODE)
    ;
fragment UNICODE
    : 'u' HEX HEX HEX HEX
    ;
fragment HEX
    : [0-9a-fA-F]
    ;
fragment SAFECODEPOINT
    : ~ ["\\\u0000-\u001F]
    ;

fragment INT
    : '0' | [1-9] [0-9]*
    ;

fragment EXP
   : [Ee] [+\-]? INT
   ;


LPAREN
    : '(' { nesting++; }
    ;

RPAREN
    : ')' { nesting--; }
    ;

LBRACK
    : '[' { nesting++; }
    ;

RBRACK
    : ']' { nesting--; }
    ;

IGNORE_NEWLINE
    : '\r'? '\n' { nesting > 0 }? -> skip
    ;

NL
    : '\r'? '\n'
    ;

WS
    : [ \t]+ -> skip
    ;

COMMENT
    : '//' ~[\r\n]* '\r'? '\n' -> type(NL)
    ;

