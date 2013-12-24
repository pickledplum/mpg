%{
#include <cstdio>
#include <string>
#include "node.h"
#include "parser.hpp"
#define STRING_TOKEN yylval.string = new std::string(yytext,yyleng);
#define CHAR_TOKEN yylval.character = yytext[0];
#define TOKEN(t) ( yylval.token = t)
extern "C" int yywrap() { }
%}

%%
[ \n\t]			;
"ABS"					STRING_TOKEN; return ABS;
"MAX"					STRING_TOKEN; return MAX;
"MIN"					STRING_TOKEN; return MIN;
[a-zA-Z_][a-zA-Z0-9_]*	STRING_TOKEN; return FSPARAM;
[0-9]+\.[0-9]*			STRING_TOKEN; return DOUBLE;
[0-9]+					STRING_TOKEN; return INTEGER;
"("						return TOKEN(LPAREN);
")"						return TOKEN(RPAREN);
"+"						CHAR_TOKEN(PLUS); return PLUS;
"-"						CHAR_TOKEN(MINUS); return MINUS;
"*"						CHAR_TOKEN(MUL); return MUL;
"/"						CHAR_TOKEN(DIV); return DIV;
","						return TOKEN(COMMA);
.						printf("Unknown token!\n"); yyterminate();

%%
