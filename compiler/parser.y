%{
	#include <cstdio>
	#include <map>
	#include "node.h"
	extern int yylex();
	void yyerror(const char*s) { printf("ERROR: %s\n", s); }
	SymbolTable *symbol_table;
	Expression *expr;
%}

%union{
	Node *node;
	Terminal * terminal;
	Expression * expr;
	std::string *string;
	char character;
	int token;
	float numeric;
}

%token <string> IDENTIFIER INTEGER DOUBLE
%token <token> LPAREN RPAREN COMMA SEMI END
%token <character> PLUS MINUS MUL DIV 
%token <string> ABS MIN MAX


%type <expr> expr
%type <character> op 
%type <terminal> ident number terminal
%type <string> unary_func binary_func

%left PLUS MINUS
%left MUL DIV
%left NEG

%start input

%%
input : { symbol_table = new SymbolTable(); } expr
	{ 
		expr = $2;
		printf("\nBefore substitution... %f\n", $2->eval()); 
		printf("Leaving yyparse()\n");
		YYACCEPT;
	}
	;
ident : IDENTIFIER { Identifier* obj = new Identifier(*$1);  (*symbol_table)[*$1] = obj;  $$ = obj; }
	;
number : INTEGER { $$ = new Numeric(atoi($1->c_str())); delete $1; }
	| DOUBLE { $$ = new Numeric(atof($1->c_str())); delete $1; }
	;

terminal : number 
	;
expr :  terminal { $$ = new Literal(*$1); }
	| expr op expr { $$ = new BinaryOperator(*$1,$2,*$3); }
	| '-' expr %prec NEG { $$ = new BinaryOperator( *(new Literal(*(new Numeric(-1)))), '*', *$2); }
	| LPAREN expr RPAREN { $$ = $2; }
//	| unary_func LPAREN expr RPAREN { $$ = new UnaryFunction(*$1,*$3); }
//	| binary_func LPAREN expr COMMA expr RPAREN { $$ = new BinaryFunction(*$3,*$1,*$5); }
	;
	
op :	PLUS | MINUS | MUL | DIV
	;
unary_func : ABS
	;
binary_func : MAX | MIN
	;

%%
