## This was copied from SIGAR CMakeLists.txt and converted to a macro
## sigar has some base files + a set of platform specific files

INCLUDE(CheckCSourceCompiles)

MACRO (CHECK_STRUCT_MEMBER _STRUCT _MEMBER _HEADER _RESULT)
   SET(_INCLUDE_FILES)
   FOREACH (it ${_HEADER})
      SET(_INCLUDE_FILES "${_INCLUDE_FILES}#include <${it}>\n")
   ENDFOREACH (it)

   SET(_CHECK_STRUCT_MEMBER_SOURCE_CODE "
${_INCLUDE_FILES}
int main()
{
   static ${_STRUCT} tmp;
   if (sizeof(tmp.${_MEMBER}))
      return 0;
  return 0;
}
")
   CHECK_C_SOURCE_COMPILES("${_CHECK_STRUCT_MEMBER_SOURCE_CODE}" ${_RESULT})

ENDMACRO (CHECK_STRUCT_MEMBER)

MACRO (GatherSIGAR prefix)
## linux
IF(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  SET(SIGAR_SRC ${prefix}/os/linux/linux_sigar.c)

  INCLUDE_DIRECTORIES(${prefix}/os/linux/)
ENDIF(CMAKE_SYSTEM_NAME STREQUAL "Linux")

## macosx, freebsd
IF(CMAKE_SYSTEM_NAME MATCHES "(Darwin|FreeBSD)")
  SET(SIGAR_SRC ${prefix}/os/darwin/darwin_sigar.c)

  INCLUDE_DIRECTORIES(${prefix}/os/darwin/)
  IF(CMAKE_SYSTEM_NAME MATCHES "(Darwin)")
    ADD_DEFINITIONS(-DDARWIN)
    SET(SIGAR_LINK_FLAGS "-framework CoreServices -framework IOKit")
  ELSE(CMAKE_SYSTEM_NAME MATCHES "(Darwin)")
    ## freebsd needs libkvm
    SET(SIGAR_LINK_FLAGS "-lkvm")
  ENDIF(CMAKE_SYSTEM_NAME MATCHES "(Darwin)")
ENDIF(CMAKE_SYSTEM_NAME MATCHES "(Darwin|FreeBSD)")

## solaris
IF (CMAKE_SYSTEM_NAME MATCHES "(Solaris|SunOS)" )
  SET(SIGAR_SRC
	${prefix}/os/solaris/solaris_sigar.c
	${prefix}/os/solaris/get_mib2.c
	${prefix}/os/solaris/kstats.c
	${prefix}/os/solaris/procfs.c
  )

  INCLUDE_DIRECTORIES(${prefix}/os/solaris/)
  ADD_DEFINITIONS(-DSOLARIS)
  SET(SIGAR_LINK_FLAGS -lkstat -ldl -lnsl -lsocket -lresolv)
ENDIF(CMAKE_SYSTEM_NAME MATCHES "(Solaris|SunOS)" )

## solaris
IF (CMAKE_SYSTEM_NAME MATCHES "(hpux)" )
  SET(SIGAR_SRC ${prefix}/os/hpux/hpux_sigar.c)
  INCLUDE_DIRECTORIES(${prefix}/os/hpux/)
  ADD_DEFINITIONS(-DSIGAR_HPUX)
  SET(SIGAR_LINK_FLAGS -lnm)
ENDIF(CMAKE_SYSTEM_NAME MATCHES "(hpux)" )

## aix
IF (CMAKE_SYSTEM_NAME MATCHES "(AIX)" )
  SET(SIGAR_SRC ${prefix}/os/aix/aix_sigar.c)

  INCLUDE_DIRECTORIES(${prefix}/os/aix/)
  SET(SIGAR_LINK_FLAGS -lodm -lcfg)
ENDIF(CMAKE_SYSTEM_NAME MATCHES "(AIX)" )

IF(WIN32)
  ADD_DEFINITIONS(-DSIGAR_SHARED)
  SET(SIGAR_SRC ${prefix}/os/win32/peb.c ${prefix}/os/win32/win32_sigar.c)
  INCLUDE_DIRECTORIES(${prefix}/os/win32)
  CHECK_STRUCT_MEMBER(MIB_IPADDRROW wType "windows.h;iphlpapi.h" wType_in_MIB_IPADDRROW)
  add_definitions(-DHAVE_MIB_IPADDRROW_WTYPE=${wType_in_MIB_IPADDRROW})
ENDIF(WIN32)

INCLUDE_DIRECTORIES(${prefix}/../include)
SET(SIGAR_SRC ${SIGAR_SRC}
  ${prefix}/sigar.c
  ${prefix}/sigar_cache.c
  ${prefix}/sigar_fileinfo.c
  ${prefix}/sigar_format.c
  ${prefix}/sigar_getline.c
  ${prefix}/sigar_ptql.c
  ${prefix}/sigar_signal.c
  ${prefix}/sigar_util.c
)

ENDMACRO(GatherSIGAR)
