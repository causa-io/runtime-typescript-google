import type { User } from '@causa/runtime';
import type { AppFixture, Fixture } from '@causa/runtime/nestjs/testing';
import { Auth } from 'firebase-admin/auth';
import jwt from 'jsonwebtoken';
import * as uuid from 'uuid';

/**
 * A {@link Fixture} to create and delete Identity Platform users in the emulator.
 * It also creates an ID token for them.
 */
export class AuthUsersFixture implements Fixture {
  /**
   * The parent {@link AppFixture}.
   */
  private appFixture!: AppFixture;

  /**
   * The list of created users.
   */
  readonly users: User[] = [];

  /**
   * The base payload required to form a JWT.
   */
  private readonly jwtBasePayload: Record<string, string>;

  /**
   * The Firebase Auth client to use.
   * This is lazily initialized when the `auth` property is accessed, which avoids trying to fetch it during
   * {@link AuthUsersFixture.init} and / or {@link AuthUsersFixture.delete}.
   */
  private lazyAuth?: Auth;

  /**
   * Creates a new {@link AuthUsersFixture}.
   */
  constructor() {
    const projectId = process.env.GOOGLE_CLOUD_PROJECT ?? '';
    this.jwtBasePayload = {
      aud: projectId,
      iss: `https://securetoken.google.com/${projectId}`,
    };
  }

  async init(appFixture: AppFixture): Promise<undefined> {
    this.appFixture = appFixture;
  }

  /**
   * The Firebase Auth client to use.
   */
  get auth(): Auth {
    if (!this.lazyAuth) {
      this.lazyAuth = this.appFixture.get(Auth);
    }

    return this.lazyAuth;
  }

  /**
   * Creates a new user if it doesn't already exist, and generates a corresponding JWT.
   *
   * @param partialUser Properties of a {@link User}.
   * @param tokenOptions Options to pass to `jsonwebtoken` when signing the token.
   * @returns The user itself, and a corresponding JWT.
   */
  async createAuthUserAndToken(
    partialUser: Partial<User> = {},
    tokenOptions: jwt.SignOptions = {},
  ): Promise<{ user: User; token: string }> {
    const { id: userId, ...customClaims } = partialUser;
    const user: User = {
      id: userId ?? uuid.v4(),
      ...customClaims,
    };

    try {
      await this.auth.createUser({ uid: user.id });
    } catch {}

    await this.auth.setCustomUserClaims(user.id, customClaims);

    this.users.push(user);

    const token = jwt.sign(
      { ...this.jwtBasePayload, sub: user.id, ...customClaims },
      null,
      { ...tokenOptions, algorithm: 'none' },
    );

    return { user, token };
  }

  async clear(): Promise<void> {}

  /**
   * Deletes all users created by this fixture.
   * This does not delete the fixture itself.
   */
  async deleteUsers(): Promise<void> {
    if (this.users.length === 0) {
      return;
    }

    await this.auth.deleteUsers(this.users.map(({ id }) => id));
    this.users.length = 0;
  }

  async delete(): Promise<void> {
    await this.deleteUsers();
    this.appFixture = undefined as any;
  }
}
