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
{
}
Terminal::~Terminal() 
{
}
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
	return value; 
}
FSParam::FSParam( const std::string& name, float value ) 
: Terminal(value), name(name) 
{
}
FSParam::FSParam( const std::string& name ) 
: name(name) 
{
}
float FSParam::eval() 
{ 
	return value; 
}
FSParam::~FSParam()
{}
BinaryOperator::BinaryOperator(Node * lhs, char op, Node * rhs) 
: lhs(lhs),op(op),rhs(rhs) 
{
}

BinaryOperator::~BinaryOperator()
{
	delete lhs;
	delete rhs;
}
float BinaryOperator::eval() 
{
	float l = lhs->eval();
	float r = rhs->eval();
	switch(op) {
		case '-': return l - r;
		case '+':  return l + r;
		case '/':   return l / r;
		case '*':   return l * r;
	}
}

UnaryFunction::UnaryFunction(const std::string& op, Node* expr) 
: op(op), expr(expr) 
{}
UnaryFunction::~UnaryFunction()
{
	delete expr;
}
float UnaryFunction::eval() 
{
	float val = expr->eval();
	if( op == "ABS" ) return fabs(val);
	throw op;
}

BinaryFunction::BinaryFunction(const std::string& op, Node* lhs, Node* rhs)
: lhs(lhs),op(op),rhs(rhs){}
BinaryFunction::~BinaryFunction()
{
	delete lhs;
	delete rhs;
}
float BinaryFunction::eval() 
{
	float l = lhs->eval();
	float r = rhs->eval();
	if( op == "MAX" ) return std::max(l,r);
	if( op == "MIN" ) return std::min(l,r);
	throw op;
}

