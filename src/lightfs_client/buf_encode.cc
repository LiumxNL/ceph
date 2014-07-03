
//test bufferlist & encoding usage()
/*
  1. stirng, int, char *, array, bufferlist encoding functions
  2. class 
  3. bufferlist iterator 
*/

#include "include/buffer.h"
#include "include/encoding.h"
#include <iostream>
#include <string>
#include <string.h>

using namespace std;

static bufferlist bl;

int test_string()
{
  uint32_t ver = 1;
  uint32_t timeout = 1;
  const char *str = "this is a string";  
  int len = strlen(str);
  string mystr(str);
  cout << "mystr.c_str = " << mystr.c_str() << endl;
  int pos = 15;
  pos = pos % len;
  bufferlist in;
  ::encode(ver, bl);
  ::encode(timeout,bl);
  in.append(mystr); //the same as ::encode_nohead(mystr, in)
  //::encode_nohead(mystr,in);
  ::encode(in, bl);

  uint32_t var1, var2;
  bufferlist new_bl;
  bufferlist::iterator bp = bl.begin();
  cout << "before decode" << endl;
  ::decode(var1, bp);
  ::decode(var2, bp);
  ::decode(new_bl, bp);
  cout << "after decode" << endl;
  cout << "var1 = " << var1 << " var2 = " << var2 << " new_bl.c_str = " << new_bl.c_str() << endl;
  
 /* 
  cout << "bl.c_str = " << bl.c_str() << " strlen(str) = " << strlen(str) << " bl.length = " << bl.length() << endl;
  int i = 0;
  for (i=0; i<bl.length(); i++ )
    cout << bl[i];
  cout << endl;
  cout << "bl[" << pos << "] = " << bl[pos] << endl;
*/
}

int main(int argc, char *argv[])
{
  test_string();
  return 0;
}
