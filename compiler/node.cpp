#include <cstdio>
#include <string>
#include <exception>
#include <cmath>
#include <cfloat>
#include "node.h"

 void print_table( const SymbolTable & table ) {
	SymbolTable::const_iterator itr = table.begin();
	while(itr != table.end() ){
		printf("%s : %f\n", itr->second->name.c_str(), itr->second->value);
		itr++;
	}
}
Node::~Node()
{}
Expression::~Expression()
{}
Terminal::Terminal() 
{}
Terminal::Terminal(float value) 
: value(value) 
{}
Terminal::~Terminal() 
{}
void Terminal::setValue( float value ) 
{ 
	this->value = value; 
}

Numeric::Numeric( float value ) 
: Terminal(value)
{}
Numeric::~Numeric() 
{}
float Numeric::eval() 
{
	printf("[%f] ", value);
	return value; 
}
Identifier::Identifier( const std::string& name, float value ) 
: Terminal(value), name(name) 
{}
Identifier::Identifier( const std::string& name ) 
: name(name) 
{}
float Identifier::eval() 
{ 
	printf("<%f> ", value);
	return value; 
}
Identifier::~Identifier()
{}
Literal::Literal(Terminal& term) 
: term(term)
{}
float Literal::eval()
{ 
	return term.eval(); 
}
Literal::~Literal()
{}
BinaryOperator::BinaryOperator(Expression & lhs, char op, Expression & rhs) 
: lhs(lhs),op(op),rhs(rhs) 
{}
BinaryOperator::~BinaryOperator()
{}
float BinaryOperator::eval() 
{
	float l = lhs.eval();
	float r = rhs.eval();
	printf("(%f %c %f) ", l, op, r);
	switch(op) {
		case '-': return lhs.eval() - rhs.eval();
		case '+':  return lhs.eval() + rhs.eval();
		case '/':   return lhs.eval() / rhs.eval();
		case '*':   return lhs.eval() * rhs.eval();
	}
}
UnaryFunction::UnaryFunction(const std::string& op, Expression& expr) 
: op(op), expr(expr) 
{}
UnaryFunction::~UnaryFunction()
{}
float UnaryFunction::eval() 
{
	float val = expr.eval();
	printf("%s(%f) ", op.c_str(), val);
	if( op == "ABS" ) return fabs(expr.eval());
	throw op;
}
BinaryFunction::BinaryFunction(Expression& lhs, const std::string& op, Expression& rhs)
: lhs(lhs),op(op),rhs(rhs){}
BinaryFunction::~BinaryFunction()
{}
float BinaryFunction::eval() 
{
	float l = lhs.eval();
	float r = rhs.eval();
	printf("%s(%f,%f) ", op.c_str(), l, r);
	if( op == "MAX" ) return std::max(l,r);
	if( op == "MIN" ) return std::min(l,r);
	throw op;
}

