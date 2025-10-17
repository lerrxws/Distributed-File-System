package lockcache

type LockState string

const (
    None      LockState = "None"
    Free      LockState = "Free"
    Locked    LockState = "Locked"
    Acquiring LockState = "Acquiring"
    Releasing LockState = "Releasing"
)
