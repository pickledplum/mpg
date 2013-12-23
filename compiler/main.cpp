#include <cstdio>
#include <cstdlib>
#include "node.h"
extern FILE* yyin;
extern int yyparse();
extern SymbolTable *symbol_table;
extern Expression * expr;

int main(int argc, char **argv)
{
	yyin = fopen("junk.in", "r");
    yyparse();
	print_table(*symbol_table);
	(*symbol_table)["P1"]->setValue(10);
	(*symbol_table)["P2"]->setValue(60);

	printf("After substitution: %f\n", expr->eval());
		print_table(*symbol_table);
    return 0;
}
