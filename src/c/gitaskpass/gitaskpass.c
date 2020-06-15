#include <stdio.h>
#include <stdlib.h>
int main()
{
	const char* s = getenv("GIT_PASSWORD");
	printf("%s", (s!=NULL)? s : "");
	unsetenv("GIT_PASSWORD");
}
