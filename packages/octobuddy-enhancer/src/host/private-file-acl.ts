import { dlopen, FFIType, read, type Pointer } from "bun:ffi";

interface AclLibrary {
  symbols: {
    acl_get_fd_np(fileDescriptor: number, type: number): Pointer | bigint | null;
    acl_free(acl: Pointer | bigint | null): number;
    __error(): Pointer | bigint | null;
  };
}

const LIBSYSTEM_PATH = "/usr/lib/libSystem.B.dylib";
const ACL_TYPE_EXTENDED = 0x100;
const ENOENT = 2;

function openAclLibrary(): AclLibrary {
  return dlopen(LIBSYSTEM_PATH, {
    acl_get_fd_np: {
      args: [FFIType.i32, FFIType.i32],
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
}

let aclLibrary: AclLibrary | null | undefined;

function getAclLibrary(): AclLibrary | null {
  if (aclLibrary !== undefined) return aclLibrary;

  try {
    aclLibrary = openAclLibrary();
  } catch {
    aclLibrary = null;
  }
  return aclLibrary;
}

/** Returns true when ACL state cannot be proven safe or an extended ACL exists. */
export async function hasExtendedAcl(fileDescriptor: number): Promise<boolean> {
  if (process.platform !== "darwin") return false;

  try {
    const library = getAclLibrary();
    if (library === null) return true;

    const acl = library.symbols.acl_get_fd_np(fileDescriptor, ACL_TYPE_EXTENDED);
    if (acl !== null && acl !== 0 && acl !== 0n) {
      library.symbols.acl_free(acl);
      return true;
    }

    const errnoPointer = library.symbols.__error();
    return (
      errnoPointer === null ||
      errnoPointer === 0 ||
      errnoPointer === 0n ||
      read.i32(errnoPointer) !== ENOENT
    );
  } catch {
    return true;
  }
}
