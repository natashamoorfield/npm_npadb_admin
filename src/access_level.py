from typing import Union


class AccessLevel(object):
    """
    The access level of a user dictates what actions she is permitted to carry out.
    The access level of an action dictates who is permitted to carry out that action.
    """
    level_descriptors = [
        'guest',
        'browser',
        'user',
        'contributor',
        'reporter',
        'collator',
        'editor',
        'adjudicator',
        'moderator',
        'custodian',
        'curator',
        'administrator',
        'engineer',
        'designer',
        'developer',
        'Natasha'
    ]
    group_descriptors = [
        'user',
        'collator',
        'moderator',
        'administrator',
        'developer',
        'Natasha'
    ]

    @staticmethod
    def is_valid_level(level: int) -> bool:
        return level in range(16)


class ActionLock(AccessLevel):

    def __init__(self, levels: Union[int, list]):
        """
        Although actions and their access levels are mainly hierarchical (that is, higher level users can execute all
        the tasks that lower level users can plus those graded at their own level), there are some tasks that
        administrators might carry out (such as assigning user privileges) that developers should not and vice versa
        (such as altering database table structures).
        However, to admit the possibility of tasks that both administrators and developers can execute, tasks can be
        given more than one access level.

        :param levels: Can be a single level value or a list of level values.
        Duplicate and invalid values are discarded.
        An empty list or one comprising only invalid items results in a task lock of zero: a task no one can execute.
        """
        if not isinstance(levels, list):
            levels = [levels]
        task_set = set()
        for item in levels:
            if self.is_valid_level(item):
                task_set.add(item)
        self.levels = sorted(task_set)

    @property
    def lock(self):
        return sum([2 ** x for x in self.levels])

    @property
    def group_ids(self):
        return [x // 3 for x in self.levels]

    @property
    def group_names(self):
        return [self.group_descriptors[x] for x in self.group_ids]

    @property
    def level_names(self):
        return [self.level_descriptors[x] for x in self.levels]


class UserKey(AccessLevel):

    def __init__(self, level):
        if self.is_valid_level(level):
            self.level = level
        else:
            # default to a guest user
            self.level = 0

    @property
    def key(self) -> int:
        """
        Users' binary access keys 'unlock' functions by having a one set in the appropriate position.  The higher up the
        hierarchy a user is, the more bits are set to one and thus the more functions they can access.  By and large
        higher level users can execute all the tasks lower level users can as well as those graded at their own level.
        However, there are some tasks administrators might carry out (such as assigning user privileges) that developers
        should not and vice versa (such as altering database table structures).
        To facilitate this, users in group 4 (developers) are denied access to functions in group 3 (administrators) by
        the subtraction of 3585 which leaves bits 10, 11 and 12 of their access keys (the bits required to access
        administrator functions) set to zero.
        :return: A binary encoded list of function levels that a user is entitled to execute.
        """
        if self.group_id == 4:
            return (2 ** (self.level + 1)) - 3585
        else:
            return (2 ** (self.level + 1)) - 1

    @property
    def group_id(self):
        return self.level // 3

    @property
    def group_name(self):
        return self.group_descriptors[self.group_id]

    @property
    def level_name(self):
        return self.level_descriptors[self.level]

    def can_do(self, task: ActionLock) -> bool:
        """
        Return true if the user's access level (her 'key') will unlock the action's task_lock.
        A user may perform an action if her user_key & (bitwise and) the action's task_lock evaluate to True
        """
        return bool(self.key & task.lock)


if __name__ == "__main__":

    for i in range(17):
        user_level = UserKey(i)
        task_level = ActionLock(i)
        print('{:>2} {:>2} {} {:14} {:14} {:>7} {:>7} {:>16} {:>16}'.format(
            i,
            user_level.level,
            user_level.group_id,
            user_level.group_name,
            user_level.level_name,
            user_level.key,
            task_level.lock,
            bin(user_level.key)[2:],
            bin(task_level.lock)[2:]
        ))
    print()

    print('t =                   ', end='')
    for i in range(16):
        print(f'{i:>3} ', end='')
    print()
    for u in range(16):
        user_level = UserKey(u)
        print(f'u = {u:>2} {user_level.level_name:14}  ', end='')
        for t in range(16):
            task_level = ActionLock(t)
            if user_level.can_do(task_level):
                print(' Y  ', end='')
            else:
                print(' -  ', end='')
        print()
