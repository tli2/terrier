#include <unistd.h>
#include <cerrno>
#include <string>
#include <vector>

#include "common/utility.h"
#if __APPLE__
extern "C" {
#include <sys/cdefs.h>
int close$NOCANCEL(int);
};
#endif

namespace terrier {

/**
 * Close a file descriptor. On all systems supported by terrier,
 * the file descriptor is closed and no retry or error recovery is required.
 *
 * WARNING: On some systems such as HPUX, return codes such as EINTR do require
 * additional error recovery.
 *
 * @param fd - descriptor to close
 * @return int error code from close. Informational only, no action required.
 */

int terrier_close(int fd) {
  // On Mac OS, close$NOCANCEL guarantees that no descriptor leak & no need to retry on failure.
  // On linux, close will do the same.
  // In short, call close/close$NOCANCEL once and consider it done. AND NEVER RETRY ON FAILURE.
  // The errno code is just a hint. It's logged but no further processing on it.
  // Retry on failure may close another file descriptor that just has been assigned by OS with the same number
  // and break assumptions of other threads.

  int close_ret = -1;
#if __APPLE__
  close_ret = ::close$NOCANCEL(fd);
#else
  close_ret = close(fd);
#endif

  if (close_ret != 0) {
    auto error_message = terrier_error_message();
  }

  return close_ret;
}

std::string terrier_error_message() {
  std::vector<char> buffer(100, '\0');
  int saved_errno = errno;
  char *error_message = nullptr;
#if __APPLE__
  (void)strerror_r(errno, buffer.data(), buffer.size() - 1);
  error_message = buffer.data();
#else
  error_message = strerror_r(saved_errno, buffer.data(), buffer.size() - 1);
#endif

  errno = saved_errno;
  return std::string(error_message);
}
}  // namespace terrier
