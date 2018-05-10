#ifndef DRIVER_HPP
#define DRIVER_HPP

#include "benchconfig.hpp"

#ifdef HAVE_DSE_H
#include <dse.h>
#elif  HAVE_CASSANDRA_H
#include <cassandra.h>
#else
#error "No driver header file configured"
#endif

#endif // DRIVER_HPP
