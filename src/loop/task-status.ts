export function isLoopTaskAutomaticallyStartable(status: string): boolean {
  switch (status) {
    case "backlog":
    case "done":
    case "cancelled":
      return false;
    default:
      return true;
  }
}
