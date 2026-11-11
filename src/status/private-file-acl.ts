import { dlopen, FFIType, read } from "bun:ffi";

const ACL_TYPE_EXTENDED = 0x00000100;
// On macOS, acl_get_fd_np reports that no ACL exists as ENOENT.
const NO_ACL_ERRNO = 2;
const LIBSYSTEM_PATH = "/usr/lib/libSystem.B.dylib";

interface DarwinAclApi {
  hasNoExtendedAcl: (fd: number) => boolean;
}

function loadDarwinAclApi(): DarwinAclApi {
  const library = dlopen(LIBSYSTEM_PATH, {
    acl_get_fd_np: {
      args: [FFIType.i32, FFIType.u32],
      returns: FFIType.ptr,
    },
    acl_free: {
      args: [FFIType.ptr],
      returns: FFIType.i32,
    },
    __error: {
      args: [],
      returns: FFIType.ptr,
    },
  });

  return {
    hasNoExtendedAcl(fd) {
      const acl = library.symbols.acl_get_fd_np(fd, ACL_TYPE_EXTENDED);
      if (acl === null) {
        const errnoPointer = library.symbols.__error();
        if (!errnoPointer) throw new Error("could not read ACL error state");
        if (read.i32(errnoPointer, 0) === NO_ACL_ERRNO) return true;
        throw new Error("could not inspect token file ACL");
      }

      if (library.symbols.acl_free(acl) !== 0) {
        throw new Error("could not release token file ACL");
      }
      return false;
    },
  };
}

let darwinAclApi: DarwinAclApi | undefined;

export function assertNoExtendedAcl(fd: number): void {
  if (process.platform !== "darwin") return;

  const api = darwinAclApi ??= loadDarwinAclApi();
  if (!api.hasNoExtendedAcl(fd)) throw new Error("token file has an ACL");
}
